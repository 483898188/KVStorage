# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目简介

一个分布式键值存储系统，底层是从零实现的 Raft 共识算法（可作为 Raft 论文的学习/参考实现，难度低于 MIT 6.824）。注释和文档以中文为主。

## 构建与运行

依赖：`muduo`（net/base）、`protobuf`、`boost`（serialization）、`pthread`、`dl`，CMake ≥ 3.22，以及支持 **C++20** 的编译器。

```bash
# 配置 + 构建（源外构建；cmake-build-debug/ 已在 .gitignore 中）
mkdir -p cmake-build-debug && cmake -S . -B cmake-build-debug && cmake --build cmake-build-debug -j

# 格式化（clang-format，会跳过自动生成的 *.pb.h/*.pb.cc）
cmake --build cmake-build-debug --target format   # 或者：bash format.sh
```

可执行文件输出到 `bin/`，库文件输出到 `lib/`。

```bash
# 启动 3 节点集群：会为每个节点 fork 一个子进程
cd bin && ./raftCoreRun -n 3 -f test.conf

# 另开终端，用示例客户端压测集群（同样要在 bin/ 下运行）
cd bin && ./callerMain
```

**工作目录的坑：** `RpcProvider::Run`（`src/rpc/rpcprovider.cpp:67`）把 `node<i>ip=` / `node<i>port=` 硬编码追加写入**当前工作目录**下的 `test.conf`，而 `KvServer` 读取的是 `-f` 指定的配置文件路径。因此集群和客户端都必须在 `bin/` 下运行（committed 的 `test.conf` 就在那里），否则两者看到的集群成员文件不是同一个。

启动具有时序敏感性：每个 `KvServer` 构造时会先 `sleep(6)` 再连接其他节点，然后 `sleep(nodeCount - me)` 才执行 `Raft::init`；示例启动器 fork 子进程时每个间隔 1 s。各节点必须一起启动，且启动端口由启动器在 `[10000, 29999]` 中随机选取。

## 测试

项目没有测试框架，`test/` 下的文件也没有接入 CMake —— 它们都是手动编译的临时验证程序（例如 `g++ -std=c++20 test/defer_run.cpp -o /tmp/defer_run`）。Raft 行为的真正验证方式是手动跑一个集群。参见 `test/测试文件运行说明.md`。

## 架构

自底向上分层；每个 `src/*` 模块都有自己的 `CMakeLists.txt`，把源文件导出到 `src_<模块名>` 这个 cache 变量，根 `CMakeLists.txt` 再把它们聚合成静态库 `skip_list_on_raft`。注意 `src/CMakeLists.txt` 中记录的构建顺序依赖：`common` 必须排在 `rpc` 之前，因为 `rpc` 的 CMake 要用到 `src_common` 变量。

源文件是用 `aux_source_directory` 收集的，所以**新增或删除源文件后必须重新执行 cmake 配置**，只重新构建是不够的。

- **`src/skipList`** —— 仅头文件的跳表，即存储引擎 / 上层状态机（`SkipList<std::string, std::string>`）。理论上可替换为任意 KV 存储。
- **`src/fiber`** —— 协程库（`monsoon::IOManager`）以及 hook/`utils`/`timer`。`Raft` 用它的 IOManager；`config.h` 中的 `FIBER_THREAD_NUM` 决定其线程池大小。
- **`src/rpc`** —— “mprpc”，基于 muduo 的 protobuf RPC 框架（它是 Raft RPC 的**传输层**，不含业务逻辑）：
  - `RpcProvider` 通过 muduo `TcpServer` 发布 `google::protobuf::Service` 对象；`NotifyService` 注册服务，`Run(nodeIndex, port)` 阻塞在事件循环中。
  - `MprpcChannel` 是客户端 stub 侧；`MprpcController` 负责传递错误。
  - 线上格式：先是一个 `RPC::RpcHeader`（`service_name`、`method_name`、`args_size`），紧接着是序列化后的 protobuf 请求（`src/rpc/rpcheader.proto`）。
  - `MprpcConfig` 解析 `key=value` 格式的配置文件（`node0ip=...`、`node0port=...`）。
- **`src/raftRpcPro`** —— Raft 自身的 protobuf 服务定义。共两个服务：
  - `raftRpcProctoc::raftRpc`（`raftRPC.proto`）—— 节点间通信：`RequestVote`、`AppendEntries`、`InstallSnapshot`。
  - `raftKVRpcProctoc::kvServerRpc`（`kvServerRPC.proto`）—— 面向客户端：`Get`、`PutAppend`。
  - 生成的 `.pb.cc`/`.pb.h` 是**提交进仓库的**（注意 `.pb.h` 放在 `include/` 下，属于手工调整过的目录结构）。按 `example/rpcExample/rpc_example.md` 用 `protoc <file>.proto --cpp_out=.` 重新生成。
- **`src/raftCore`** —— 共识核心，以及把共识与状态机对接起来的服务层：
  - `raft.cpp/h` —— `Raft` 类：领导者选举（`doElection`、`electionTimeOutTicker`）、心跳（`doHeartBeat`、`leaderHearBeatTicker`）、日志复制（`sendAppendEntries`、`AppendEntries1`）、commitIndex 推进、快照（`Snapshot`、`leaderSendSnapShot`、`InstallSnapshot`），以及持久化（`persist`、`readPersist`，通过 boost 序列化 `BoostPersistRaftNode`）。它**继承自生成的 `raftRpcProctoc::raftRpc` 服务**，因此同一个对象既对外提供 RPC 服务又持有全部状态；被重写的 `google::protobuf::RpcController` 风格方法就是 RPC 入口。
  - `raftRpcUtil.cpp/h` —— 面向单个对端节点的出站 stub（`RaftRpcUtil`），每个远端节点一个；`Raft::m_peers` 中自己那一槽是 `nullptr`。
  - `kvServer.cpp/h` —— `KvServer`，同样是一个生成的 protobuf 服务（`kvServerRpc`）。负责把客户端 RPC → Raft 提案 → 已应用状态机这条链路串起来。
  - `Persister.cpp/h` —— 落盘的 raftstate/snapshot 文件（按节点编号命名）。
- **`src/raftClerk`** —— 客户端侧：`Clerk`（会话/去重 ID、重定向到 leader）与 `raftServerRpcUtil`（每台服务器一个 stub）。
- **`src/common`** —— `util.h`（`Op` 命令类型、`LockQueue<T>` 阻塞队列、`DEFER`、`DPrintf`、`format`）、`config.h`（所有时间参数集中在此）、`util.cpp`。

### 请求流转

1. `Clerk::Init` 读取配置文件发现所有节点，随后 `Get`/`Put`/`Append` 通过 `raftServerRpcUtil` 向 `m_recentLeaderId` 发起 `PutAppend`/`Get` 调用。
2. 遇到 `ErrWrongLeader` 或 RPC 失败时，客户端切换到下一个服务器并**用同一个 `RequestId`** 重试 —— 线性一致性由下游保证，而非客户端（见 `clerk.cpp`）。
3. `KvServer::PutAppend`/`Get` 构造 `Op`（Operation/Key/Value/ClientId/RequestId）并调用 `Raft::Start`，同时以返回的 raft index 为键，在 `waitApplyCh` 中注册一个 `LockQueue<Op>`；若本节点不是 leader，立即回复 `ErrWrongLeader`。
4. `Raft` 通过 `AppendEntries` 复制该条目，一旦提交就把 `ApplyMsg`（有 CommandValid / SnapshotValid 两种形态）推入 `applyChan` —— 一个由 `Raft` 与 `KvServer` 共享的 `LockQueue<ApplyMsg>`。
5. `KvServer::ReadRaftApplyCommandLoop`（跑在独立线程上，并在构造函数里 join，因此构造函数永不返回）把命令应用到 `SkipList`，并通过 `SendMessageToWaitChan` 唤醒等待中的 RPC。
6. 去重依赖 `m_lastRequestId[clientId]`；当 raft state 超过 `m_maxRaftState` 时，由 `IfNeedToSendSnapShotCommand` 触发快照。

`Op` 用 **boost 文本归档**序列化（`Op::asString`/`parseFromString`），并以 protobuf 的 `string` 字段传输。`KvServer::getSnapshotData`/`parseFromString` 对整个对象做 boost 序列化；状态机本身则通过 `SkipList::dump_file`/`load_file` 转储与加载。

## 约定

- `.clang-format`：基于 Google，2 空格缩进，120 列上限；自动生成的 `*.pb.*` 文件被排除在格式化之外 —— 不要重新格式化它们。
- 私有成员以 `m_` 为前缀；跳表内部成员用 `_` 前缀。
- 中文注释是常态；写注释时请与周围代码保持同样的语言和密度。
- 所有可调时间参数都在 `config.h`。`debugMul` 用于在慢网络下整体放大超时；`Debug` 控制 `DPrintf` 输出（每应用一条 op 都会调用 `DprintfKVDB`）。
- `Raft` 内部统一使用 `std::thread`（而非裸 pthread）以保证可移植性，并用 `DEFER` 管理锁/清理的作用域。
