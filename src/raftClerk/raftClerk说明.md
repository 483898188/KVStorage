# raftClerk 模块说明

> 客户端侧的 Raft KV 访问库。本文面向"学分布式、从客户端这层入手"的读者，
> 逐个文件解释 Clerk 如何在不知道 Raft 内部机制的前提下，配合服务端实现线性一致性。

## 目录

- [1. 模块定位](#1-模块定位)
- [2. 文件清单](#2-文件清单)
- [3. Clerk 的状态](#3-clerk-的状态)
- [4. 初始化：发现集群](#4-初始化发现集群)
- [5. PutAppend：请求重试循环](#5-putappend请求重试循环)
- [6. Get：与 PutAppend 的差异](#6-get与-putappend-的差异)
- [7. raftServerRpcUtil：stub 封装](#7-raftserverrpcutilstub-封装)
- [8. 线性一致性从哪来](#8-线性一致性从哪来)
- [9. 已知疑点](#9-已知疑点)
- [10. 建议的后续阅读路径](#10-建议的后续阅读路径)

---

## 1. 模块定位

在整体分层中，raftClerk 位于最上层，是用户程序直接链接的库：

```
用户程序 (example/raftCoreExample)
   │  Put / Get / Append
   ▼
Clerk                        ← 本模块，每个用户进程一个实例
   │  Errcode 驱动的重试循环
   ▼
raftServerRpcUtil            ← 本模块，把 mprpc 的 stub 包一层
   │  kvServerRPC.proto: Get / PutAppend
   ▼
KvServer (某个节点)           ← src/raftCore
   │  Raft::Start
   ▼
Raft 共识 → SkipList 状态机
```

**关键认知：Clerk 完全不知道 Raft 的存在。** 它只会三种动作：

1. 向某台服务器发一次 KV 请求；
2. 收到 `ErrWrongLeader` 或 RPC 失败，就换一台重发；
3. 收到 `OK`，就记住这台服务器是 leader，下次优先打它。

选举、日志复制、提交推进、快照，对客户端全部不可见。

---

## 2. 文件清单

| 文件 | 作用 |
|---|---|
| `include/clerk.h` | `Clerk` 类声明：对外暴露 `Init` / `Get` / `Put` / `Append` |
| `clerk.cpp` | 重试循环的全部实现 |
| `include/raftServerRpcUtil.h` | 单个远端节点的 stub 封装声明 |
| `raftServerRpcUtil.cpp` | stub 构造与错误归一化 |
| `CMakeLists.txt` | 导出 `src_raftClerk`，由根 CMakeLists 聚合进 `skip_list_on_raft` |

---

## 3. Clerk 的状态

`include/clerk.h:20-24`，全部状态只有四个成员：

| 字段 | 类型 | 含义 |
|---|---|---|
| `m_servers` | `vector<shared_ptr<raftServerRpcUtil>>` | 每台 raft 节点一个 stub，下标即本地编号 |
| `m_clientId` | `std::string` | 随机生成的客户端唯一 ID，服务端用它做**去重** |
| `m_requestId` | `int` | 单调递增的请求序号 |
| `m_recentLeaderId` | `int` | "上一次我以为的 leader"，只是**缓存/猜测** |

`m_clientId` 与 `m_requestId` 组成全局唯一的请求标识 `(clientId, requestId)`。

> **核心洞察**：整个线性一致性保证，建立在"重试时这两个值不变"之上。
> 客户端不保证请求不重复执行，它保证的是**重试用的是同一个 ID**，剩下的交给服务端。

构造函数 `clerk.cpp:103`：

```cpp
Clerk::Clerk() : m_clientId(Uuid()), m_requestId(0), m_recentLeaderId(0) {}
```

`Uuid()` 由四次 `rand()` 拼接而成（`include/clerk.h:26-28`）。

---

## 4. 初始化：发现集群

`clerk.cpp:78-101`

```cpp
void Clerk::Init(std::string configFileName) {
  MprpcConfig config;
  config.LoadConfigFile(configFileName.c_str());

  std::vector<std::pair<std::string, short>> ipPortVt;
  for (int i = 0; i < INT_MAX - 1; ++i) {
    std::string node = "node" + std::to_string(i);
    std::string nodeIp = config.Load(node + "ip");
    std::string nodePortStr = config.Load(node + "port");
    if (nodeIp.empty()) break;                 // 读到空 → 节点数到此为止
    ipPortVt.emplace_back(nodeIp, atoi(nodePortStr.c_str()));
  }

  for (const auto& item : ipPortVt) {          // 每台建一个 stub
    auto* rpc = new raftServerRpcUtil(item.first, item.second);
    m_servers.push_back(std::shared_ptr<raftServerRpcUtil>(rpc));
  }
}
```

要点：

- 配置格式是 `node<i>ip=` / `node<i>port=`，从 `i = 0` 开始连续读到第一个空值为止 —— **"读到空就停"就是节点数量的判定方式**。
- 循环写成 `for (int i = 0; i < INT_MAX - 1; ++i)` 而非 `while (true)`，逻辑等价，属于风格怪癖。
- **配置文件是运行时被改写的**：`RpcProvider::Run`（`src/rpc/rpcprovider.cpp:67`）会把实际的 `node<i>ip/port` 追加写入**当前工作目录**下的 `test.conf`，而 `KvServer` 读的是 `-f` 指定的路径。端口在 `[10000, 29999]` 内随机选取。
- 因此**集群和客户端必须在同一工作目录下运行**（committed 的 `test.conf` 在 `bin/`），否则两边看到的成员列表不是同一份。

---

## 5. PutAppend：请求重试循环

`clerk.cpp:41-72` —— 全模块最值得逐行读的代码。

```cpp
void Clerk::PutAppend(std::string key, std::string value, std::string op) {
  m_requestId++;                        // ① 序号只在"进入新操作"时加一次
  auto requestId = m_requestId;         //    整个 while 内 requestId 不变
  auto server = m_recentLeaderId;       // ② 从猜的 leader 开始

  while (true) {
    raftKVRpcProctoc::PutAppendArgs args;
    args.set_key(key);
    args.set_value(value);
    args.set_op(op);
    args.set_clientid(m_clientId);
    args.set_requestid(requestId);

    raftKVRpcProctoc::PutAppendReply reply;
    bool ok = m_servers[server]->PutAppend(&args, &reply);

    if (!ok || reply.err() == ErrWrongLeader) {     // ③ 失败 或 "我不是 leader"
      server = (server + 1) % m_servers.size();     //    换一台
      continue;                                     //    同一个 requestId 重发
    }
    if (reply.err() == OK) {
      m_recentLeaderId = server;                    // ④ 记住 leader，下次直接打
      return;
    }
  }
}
```

四个要点：

1. **`m_requestId++` 在循环外** —— 最关键的一行。重试不产生新请求，只是同一请求再次投递。
   若 RPC 实际已到达并被提交、仅响应丢失，服务端靠 `(clientId, requestId)` 认出重复，返回**缓存的结果**而非再次执行。
   对 `Append` 而言，重复执行会直接破坏正确性。
2. **`!ok` 与 `ErrWrongLeader` 同等对待**，但来源不同：
   - `!ok`：`MprpcController::Failed()` 为真 —— RPC 层失败（连不上、超时），见 `raftServerRpcUtil.cpp:24-31`；
   - `ErrWrongLeader`：RPC 成功，业务层拒绝 —— `KvServer` 发现本节点不是 leader。
   两者在客户端看来都是"可恢复、换一台重试"。
3. **轮转起点是 `m_recentLeaderId`** —— 乐观缓存。集群稳定时绝大多数请求一次命中，零额外跳转；leader 挂了才开始 `+1` 扫描。
4. **`(server + 1) % size` 是轮转，不是"向已知 leader 靠拢"**。客户端手上没有 Raft 的 `leaderId`，只能暴力扫描所有节点。

`Put` / `Append` 只是 op 参数不同的薄封装（`clerk.cpp:74-76`）：

```cpp
void Clerk::Put(std::string key, std::string value) { PutAppend(key, value, "Put"); }
void Clerk::Append(std::string key, std::string value) { PutAppend(key, value, "Append"); }
```

---

## 6. Get：与 PutAppend 的差异

`clerk.cpp:12-39`，结构完全一致，只多一个 `ErrNoKey` 分支：

```cpp
if (reply.err() == ErrNoKey) return "";
if (reply.err() == OK) { m_recentLeaderId = server; return reply.value(); }
```

**容易忽略的点：Get 也是发给 leader、也走 `Raft::Start` 变成日志的。**

这不是"读本地状态机"，而是线性一致读 —— 读也要经过共识。这是最简单的一种实现，代价是读延迟等于一次日志复制，好处是无需处理 `readIndex` / lease read 那套机制。你可以对照 `KvServer::Get` 验证这一点。

---

## 7. raftServerRpcUtil：stub 封装

`raftServerRpcUtil.cpp`

```cpp
raftServerRpcUtil::raftServerRpcUtil(std::string ip, short port) {
  stub = new raftKVRpcProctoc::kvServerRpc_Stub(new MprpcChannel(ip, port, false));
}

bool raftServerRpcUtil::Get(GetArgs *args, GetReply *reply) {
  MprpcController controller;               // 每次调用新建，避免状态串扰
  stub->Get(&controller, args, reply, nullptr);
  return !controller.Failed();              // 把错误归一化成 bool
}
```

它只做两件事：**建 stub**、**把 `MprpcController` 的错误折叠成 `bool`**。

文件顶部注释点明了它与 raft 节点间通信实现的关键区别：

> kvserver 不同于 raft 节点之间，kvserver 的 rpc 是用于 clerk 向 kvserver 调用，**不会被调用**，
> 因此只用写 caller 功能，不用写 callee 功能。

对比 `src/raftCore/raftRpcUtil.h`：那边**同时**有 caller 和 callee，因为 Raft 节点之间是双向的（A 给 B 发 `AppendEntries`，B 也会给 A 发 `RequestVote`）。而 `KvServer` 只被动接受客户端请求，所以这里**不需要**继承 protobuf 生成的 Service 类。

这也解释了 CLAUDE.md 里那句：`Raft` 继承生成的 `raftRpcProctoc::raftRpc`，同一个对象既提供 RPC 服务又持有全部状态。在客户端这层看不出来，读 `raftCore` 时才显现。

---

## 8. 线性一致性从哪来

把责任划分理清楚，这是本模块最重要的收获：

| 保证 | 由谁负责 | 机制 |
|---|---|---|
| 不重复执行 | **服务端** | `KvServer` 的 `m_lastRequestId[clientId]`；命中则重放缓存结果 |
| 不丢请求 | **客户端** | `while(true)` 无限重试，换服务器直到收到非 `ErrWrongLeader` |
| 请求唯一标识 | **客户端** | `(m_clientId, m_requestId)`，重试期间不变 |
| 操作全序 | **Raft** | 日志索引 + commitIndex |
| 找 leader | **客户端** | 暴力轮转 + `m_recentLeaderId` 缓存 |

客户端是"**至少一次**"投递，服务端做去重后变成"**恰好一次**"的效果。这就是 Raft 客户端语义的标准做法。

`src/common/include/util.h:180-182` 定义了全部三种返回码：

```cpp
const std::string OK = "OK";
const std::string ErrNoKey = "ErrNoKey";
const std::string ErrWrongLeader = "ErrWrongLeader";
```

---

## 9. 已知疑点

以下是阅读中发现的、**建议自己动手验证**的问题（未实际运行集群验证）：

1. **无限循环的边界**：两个 `while(true)` 只处理了 `ErrWrongLeader` / `ErrNoKey` / `OK`。
   若服务端返回其它错误值，客户端会**死循环**。`PutAppend` 尤其明显 —— 连 `ErrNoKey` 分支都没有。
   当前因为只有三个常量所以不会触发，但结构上是脆弱的。
2. 🔴 **`Uuid()` 未播种 —— 已实测复现，导致静默丢写**：`rand()` 没有配套 `srand()`（`include/clerk.h:26-28`），
   所以**每个客户端进程拿到的 clientId 序列完全相同**。两个客户端进程连同一个集群时，服务端
   `m_lastRequestId[clientId]` 里已有第一个进程的进度，第二个进程的 `RequestId` 一律 `<=` 该值，
   被 `ifRequestDuplicate` 判为重复 → **写入不执行**，但去重路径照样唤醒等待者并回 **`OK`**。
   **客户端收到成功，数据却没写进去 —— 线性一致性被破坏。**

   **实测**（`benchmark/_dedup_test.sh`，同一集群 + 两个客户端进程，用值前缀区分）：

   ```
   客户端进程 #1：校验 300 个 key，丢失 0，值错误 0     ✅
   客户端进程 #2：校验 300 个 key，丢失 0，值错误 300   ❌   ← 全部没写进去
   ```

   `值错误 300` = 300/300 的 key 上仍是进程 #1 写的值，而进程 #2 每次都收到了 `OK`。

   **触发条件极其平常**：任何两个客户端进程共存于同一集群即可 —— 第二次运行 `callerMain`、
   客户端重启、两个用户同时使用。**注意这是跨进程才触发**：同一进程内建两个 `Clerk`，
  第二个会继续消耗 `rand()` 序列，拿到不同 ID。

   **修复**：`Uuid()` 里用 `std::random_device`，或至少加一次 `srand(time(nullptr) ^ getpid())`。
   `Collision` 的后果不是"重试一下就好"，而是**静默丢数据**。
3. **`m_servers` 下标与 raft node index 的关系**：由于重试是暴力轮转，即使顺序错乱最终也能找到 leader，
   只是更慢。可思考为什么让配置文件顺序与节点编号一致会更优。
4. **`m_requestId` 用 `int`**：溢出后 `(clientId, requestId)` 的唯一性还成立吗？多久会溢出？
5. **`m_servers` 用 `shared_ptr` 却用 `new` 构造**：`clerk.cpp:98-99` 可以改用 `std::make_shared`。

---

## 10. 建议的后续阅读路径

1. **`src/raftCore/kvServer.cpp`** 的 `PutAppend` / `Get`
   —— 看 `ErrWrongLeader` 究竟在哪一行产生，以及 `waitApplyCh` 那个 `LockQueue` 如何阻塞 RPC 线程等待 Raft 提交。
2. **`KvServer::ReadRaftApplyCommandLoop`**
   —— 看去重表 `m_lastRequestId` 的具体实现，验证第 8 节的责任划分。
3. **`src/raftCore/raft.cpp`** 的 `Start` → `AppendEntries` → `applyChan`
   —— 看一条提案如何变成 `ApplyMsg`。

启动实验环境（必须在 `bin/` 下执行，原因见第 4 节）：

```bash
mkdir -p cmake-build-debug && cmake -S . -B cmake-build-debug && cmake --build cmake-build-debug -j
cd bin && ./raftCoreRun -n 3 -f test.conf     # 终端 A
cd bin && ./callerMain                        # 终端 B
```

打开 `config.h` 里的 `Debug` 开关，可以看到每次换 leader 重试的日志（`clerk.cpp:56`）。
