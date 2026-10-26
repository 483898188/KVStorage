# Raft 项目面试问答：六道技术追问

> 项目：自研 Raft 共识 + 分布式 KV（`KVstorageBaseRaft-cpp`）
> 用途：面试深水区问答，配合 [面试讲稿-Raft](./面试讲稿-Raft.md) 使用
> 结构：**第一部分（Q1–Q6）** 是六道核心追问；**第二部分** 是按题纲展开的完整题库（选举、复制、快照、一致性、持久化、RPC、工程、压测、故障、扩展、陷阱题）。
> 相关文档：[leader选举](./leader选举.md)、[日志匹配](./日志匹配.md)、[持久化](./持久化.md)、[链路](./链路.md)、[面试 Bug 清单](./面试Bug清单.md)

---

## 目录

| # | 问题 | 核心结论 |
|---|---|---|
| Q1 | 持久化时序真的是 Boost.Serialization 保证的吗？ | ❌ 不是，它只做序列化；顺序由作用域守卫 + `done->Run()` 位置决定 |
| Q2 | 解决什么问题？为什么自研而不用 etcd / LogCabin？ | 定位是学习/验证；生产环境默认应选 etcd |
| Q3 | 整体架构与一次 Put 的完整链路 | 五层分工，`raftIndex` 是上下半环唯一的汇合键 |
| Q4 | 三节点部署怎么做？ | 单机多进程；多机不是改配置能实现的 |
| Q5 | 能力边界：哪些是论文、哪些是工程增强、缺什么 | 算法达标，系统属研究原型 |
| Q6 | 重做一遍先改哪个设计？为什么？ | 持久化模型：全量重写 → 追加式 WAL |

---

## Q1：持久化时序真的是 Boost.Serialization 保证的吗？

### 结论

**不是。** Boost.Serialization 只负责「对象 → 字节流」，既不提供事务语义，也不知道文件、`fsync`、网络应答的存在。把"先落盘、后应答"归因给它，是把三个层次混成了一层。

### 真正保证时序的三层机制

#### 第一层：`DEFER` 的析构顺序

```cpp
template <class F>
class DeferClass {
 public:
  ~DeferClass() { m_func(); }
};
#define DEFER DeferClass defer_placeholder##__LINE__ = [&]()
```

```cpp
void Raft::RequestVote(const RequestVoteArgs* args, RequestVoteReply* reply) {
  std::lock_guard<std::mutex> lg(m_mtx);   // ① 先构造
  DEFER { persist(); };                    // ② 后构造
  ...
  reply->set_votegranted(true);
  return;
}
```

C++ 保证局部对象**按声明顺序的逆序析构**，所以离开函数时：

```text
persist() 执行  →  lock_guard 析构（解锁）
```

即 `persist()` 一定在**解锁之前、函数返回之前**完成。这是语言层面的确定性。

#### 第二层：5 参数包装函数的 `done->Run()` 位置

```cpp
void Raft::RequestVote(controller, request, response, done) {
  RequestVote(request, response);   // ← 内部 persist() 已执行完
  done->Run();                      // ← 这时才真正序列化响应并 conn->send
}
```

`done->Run()` 是**网络应答的唯一出口**。它在 2 参数版返回之后才被调用。

四条路径都是这个形状：

| 路径 | 持久化位置 | 应答位置 |
|---|---|---|
| `RequestVote` | `DEFER`，2 参数函数返回前 | 5 参数包装函数里 `done->Run()` |
| `AppendEntries1` | `DEFER`，函数返回前 | 同上 |
| `InstallSnapshot` | 2 参数函数末尾显式 `Save()` | 同上 |
| `doElection` | `persist()` 在 spawn 拉票线程**之前** | 无应答，但先于对外发 RPC |

#### 第三层：锁保证序列化期间状态不被改

`persist()` 在 `m_mtx` 内执行（`DEFER` 比 `lock_guard` 晚声明、早析构），所以 `currentTerm`/`votedFor` 在整个序列化过程中不会被他线程修改，写到磁盘的是一份**自洽快照**。

### Boost.Serialization 在这里的真实作用

```cpp
std::string Raft::persistData() {
  BoostPersistRaftNode node;   // 填 term / votedFor / snapshot 点 / logs
  std::stringstream ss;
  boost::archive::text_oarchive oa(ss);
  oa << node;          // ① 序列化进内存
  return ss.str();     // ② oa 析构时 flush 归档，取出完整字符串
}

void Raft::persist() {
  auto data = persistData();          // ③ 完整字节串已在内存里
  m_persister->SaveRaftState(data);   // ④ 才碰文件
}
```

「先序列化到内存、再写文件」保证文件拿到的永远是完整字节串。但这是 `persistData()` 的写法带来的，**换成任何序列化器结果完全一样**。

### 两个必须承认的缺口

| 缺口 | 现状 | 后果 |
|---|---|---|
| `flush()` ≠ 落盘 | 只到内核 page cache，无 `fsync` | 扛进程崩溃，扛不住断电/内核 panic |
| 先 truncate 再写 | `clearRaftState()` 后立即写 | 窗口内崩溃 → 文件为空或残缺 |

### 面试时的正确表述

不要说：

> ❌ 用 Boost.Serialization 保证了投票和任期先落盘、后应答。

应该说：

> 时序由代码结构保证：`persist()` 通过作用域守卫在函数返回前、解锁前执行，而网络应答只在 5 参数包装函数调 `done->Run()` 时才发出。序列化本身用什么库不影响这个顺序。
> 但当前只有 `flush()` 没有 `fsync()`，且写文件是"先 truncate 再重写"，所以严格意义上**不满足 Raft 要求的 stable storage**——它扛得住进程崩溃，扛不住断电。

---

## Q2：解决什么问题？为什么自研而不是直接用 etcd / LogCabin？

### 一句话版本

> 这个项目**不解决 etcd 解决不了的问题**——它的定位是"亲手实现并验证共识"，不是"替代 etcd"。
> 如果目标是交付业务，我会直接用 etcd；我自研是为了拿到对数据通路的完全控制权，以及理解失败模式。

### 它到底解决什么问题

| 层次 | 内容 |
|---|---|
| **业务层** | 为一组机器提供**线性一致的 KV 存储**：多副本、领导者选举、崩溃恢复、日志压缩 |
| **技术层** | 把共识、持久化、快照、客户端重试与幂等**串成可运行的闭环** |
| **真实定位** | 学习与验证载体，用来回答"如果让我实现一遍，我会在哪里错" |

### 自研的合理理由

1. **目标就是理解与掌控**：etcd 是黑盒，看不到一次 `AppendEntries` 失败时 `nextIndex` 怎么回退。自研后可以**在 Raft 循环里插探针、注入故障**——这在 etcd 内部做不到。
2. **需要共设计状态机与复制层**：`Raft` 只认 `LockQueue<ApplyMsg>`，状态机和快照格式可替换。在嵌入式/边缘场景里，不想为一个 KV 引入 Go 运行时、gRPC 和独立进程。
3. **依赖与部署成本**：etcd 是独立服务，要端口、TLS、运维、监控；本项目是一个进程内嵌的复制日志。

### 必须承认的代价

| 维度 | etcd | 本项目 |
|---|---|---|
| 耐久性 | WAL + fsync + 原子 rename | 只有 flush，无 fsync |
| 成员变更 | 支持 | ❌ 启动时固定 |
| PreVote / ReadIndex | 有 | ❌ 没有 |
| 安全 | TLS / mTLS / RBAC | ❌ 无 |
| 可观测性 | metrics / tracing | ❌ 只有 DPrintf |
| 测试 | 大规模一致性/混沌测试 | ❌ 手工集群 + 脚本 |
| 客户端生态 | 多语言、watch、lease | ❌ 自研 Clerk |

### 和 etcd / LogCabin 的区别

| | etcd | LogCabin | 本项目 |
|---|---|---|---|
| 定位 | 生产级分布式 KV | Raft 作者 Ongaro 的研究原型（C++） | 学习型实现（C++） |
| 一致性 | 线性一致（ReadIndex/租约读） | Raft | 线性一致，但读也走完整共识 |
| 成员变更 | 动态 | 有 | ❌ |
| 持久化 | WAL + 快照 + fsync | 日志 + 快照 | 全量重写，无 fsync |
| 安全 | TLS/RBAC | ❌ | ❌ |

> LogCabin 是最接近的"同类项"，但它同样不是生产可用。它的存在恰好说明：**自研的价值在于过程，不在于产物。**

### 追问预案

**Q：那这个项目有什么实际价值？**

> 一是我能说清 Raft 的每个安全性不变量在代码里落在哪一行；二是我建立了能复现问题的验证手段（压测、故障注入、持久化探针），并用它定位过"崩溃恢复从未生效""客户端 ID 冲突导致静默丢写"这类问题。

**Q：如果做成可用的东西，要补什么？**

> WAL + fsync + 原子 rename（耐久性）→ PreVote 和 ReadIndex → 动态成员变更 → TLS/鉴权 → 指标与混沌测试。

---

## Q3：整体架构与一次 Put 的完整链路

### 整体架构

```text
                         ┌──────────────────────────────────┐
                         │  Clerk（客户端库, raftClerk）     │
                         │  · clientId / requestId           │
                         │  · m_recentLeaderId 记忆 + 环形重试│
                         └────────────────┬─────────────────┘
                                          │ kvServerRpc（protobuf over TCP）
                                          ▼
┌─────────────────────────── 节点进程（KvServer）─────────────────────────────┐
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ RPC 层  mprpc（muduo TcpServer + protobuf generic service）            │  │
│  │  RpcProvider: 收包 → 解析 RpcHeader → 查 service/method → 反序列化     │  │
│  │               → CallMethod 多态分发 → done->Run() 序列化回包           │  │
│  │  MprpcChannel: 客户端 Stub 侧（RaftRpcUtil / raftServerRpcUtil 使用）   │  │
│  └──────────────┬─────────────────────────────────────┬──────────────────┘  │
│                 │ kvServerRpc                         │ raftRpc             │  ▲
│                 ▼                                     ▼                    │  │
│  ┌─────────────────────────────┐      ┌──────────────────────────────┐     │  │
│  │ 状态机层  KvServer          │      │ 共识层  Raft                 │     │  │
│  │  · Op 封装 / Start 提案      │─────▶│  · 选举 / 心跳 / 日志复制     │     │  │
│  │  · waitApplyCh[raftIndex]   │ 线1  │  · commitIndex 推进          │     │  │
│  │  · 去重表 m_lastRequestId    │◀─────│  · applyChan（ApplyMsg）     │     │  │
│  │  · SkipList 状态机          │ 线2  │  · m_logs（LogEntry 日志）    │     │  │
│  └──────────────┬──────────────┘      └──────────────┬───────────────┘     │  │
│                 └─────────────────┬──────────────────┘                     │  │
│                                   ▼                                        │  │
│                  ┌──────────────────────────────────┐                      │  │
│                  │ 持久化层  Persister（线3，共享）   │                      │  │
│                  │  raftstate<i>.txt  ← Raft         │                      │  │
│                  │  snapshot<i>.txt   ← KvServer     │                      │  │
│                  └──────────────────────────────────┘                      │  │
└─────────────────────────────────────────────────────────────────────────────┘  │
     其它节点 ──────────────────── raftRpc ───────────────────────────────────────┘
```

三条线的含义：

- **线 1**：`KvServer::m_raftNode->Start(op, ...)` —— 客户端请求进入共识层。
- **线 2**：`shared_ptr<LockQueue<ApplyMsg>> applyChan` —— 已提交日志回到状态机。
- **线 3**：`shared_ptr<Persister>` —— 两层共享落盘机制，但写不同文件。

### 各层职责

| 层 | 核心类 | 职责 | **不负责** |
|---|---|---|---|
| **客户端** | `Clerk`、`raftServerRpcUtil` | 生成 `clientId` 与单调 `requestId`；选择目标节点；失败时环形切换并**用同一 requestId 重试** | 不判重、不保证线性一致 |
| **RPC 层** | `RpcProvider`、`MprpcChannel` | 分帧、序列化、服务注册与分发、连接管理、错误传递 | 不认识 Raft，也不认识 KV |
| **共识层** | `Raft` | 选举、日志复制、commit 推进、持久化、快照、冲突回退 | **不解释命令内容**；不碰 `SkipList` |
| **状态机层** | `KvServer` | 封装 `Op`、按 `raftIndex` 挂号、去重执行、生成/安装快照、唤醒 RPC | 不做共识 |
| **持久化层** | `Persister` | 文件读写、加锁、大小缓存、两条写路径 | **不定义格式** |

关键边界：同一个 `RpcProvider` 同时挂两个服务（`kvServer.cpp:391-392`）：

```cpp
provider.NotifyService(this);                    // "kvServerRpc" → 客户端流量
provider.NotifyService(this->m_raftNode.get());  // "raftRpc"   → 对端节点流量
```

对端的 `AppendEntries` **绕过 `KvServer` 直接打到 `Raft` 对象**。

### 一次 Put 的完整链路

#### 阶段 1：客户端发出

| # | 模块 | 动作 |
|---|---|---|
| 1 | `Clerk::PutAppend` | `m_requestId++`；`server = m_recentLeaderId`；填 `PutAppendArgs` |
| 2 | `raftServerRpcUtil::PutAppend` | 调用 `kvServerRpc_Stub` |
| 3 | `MprpcChannel::CallMethod` | 拼 `RpcHeader` + 序列化载荷 → TCP |

#### 阶段 2：RPC 层分发

| # | 模块 | 动作 |
|---|---|---|
| 4 | `RpcProvider::OnMessage` | 解析头部 → `m_serviceMap.find("kvServerRpc")` → 找 method |
| 5 | `RpcProvider::OnMessage` | `GetRequestPrototype().New()` → `ParseFromString`；`GetResponsePrototype().New()` |
| 6 | `RpcProvider::OnMessage` | 构造 `done = NewCallback(&SendRpcResponse, conn, response)` |
| 7 | `service->CallMethod(...)` | 进生成的 `kvServerRpc::CallMethod`，`switch(method->index())` |
| 8 | `KvServer::PutAppend(5参数)` | 调用 2 参数版（**此时还没回包**） |

#### 阶段 3：状态机 → 共识层（上行）

| # | 模块 | 动作 |
|---|---|---|
| 9 | `KvServer::PutAppend(2参数)` | 把 args 拆成 `Op` |
| 10 | `Raft::Start` | 非 leader → `isLeader=false`，回 `ErrWrongLeader` |
| 11 | `Raft::Start` | leader：构造 `LogEntry{command=op.asString(), term, index}` → 追加 `m_logs` |
| 12 | `Raft::persist` → `Persister::SaveRaftState` | **先持久化**（truncate + 重写 raftstate + flush） |
| 13 | `Raft::Start` | 返回 `raftIndex` |
| 14 | `KvServer::PutAppend` | `waitApplyCh[raftIndex] = new LockQueue<Op>()`；`timeOutPop` **阻塞** |

#### 阶段 4：复制（共识层内部）

| # | 模块 | 动作 |
|---|---|---|
| 15 | `leaderHearBeatTicker`（协程，25 ms） | 醒 → `doHeartBeat()` |
| 16 | `doHeartBeat` | 为每个 peer 构造 `AppendEntriesArgs`，起 detached 线程 |
| 17 | `sendAppendEntries` → `RaftRpcUtil` → `MprpcChannel` | 出站 raftRpc |
| 18 | 对端 `RpcProvider` → `Raft::AppendEntries(5参数)` | 绕过 `KvServer` |
| 19 | `AppendEntries1` | 校验 term → 重置选举计时器 → 校验 prev → 写日志 → 更新 `commitIndex` → `DEFER persist()` |
| 20 | `sendAppendEntries`（回复） | 更新 `matchIndex/nextIndex`；多数派且**当前 term** → 推进 `m_commitIndex` |

#### 阶段 5：应用到状态机（下行）

| # | 模块 | 动作 |
|---|---|---|
| 21 | `Raft::applierTicker`（线程，10 ms） | `getApplyLogs()` 构造 `ApplyMsg` |
| 22 | `Raft::applierTicker` | `applyChan->Push(msg)` ← **线 2** |
| 23 | `KvServer::ReadRaftApplyCommandLoop` | `applyChan->Pop()` 阻塞弹出 |
| 24 | `KvServer::GetCommandFromRaft` | `op.parseFromString`；检查快照水位 |
| 25 | `KvServer::GetCommandFromRaft` | `ifRequestDuplicate` 去重 |
| 26 | `ExecutePutOpOnKVDB` | 写入 `SkipList`，更新 `m_lastRequestId` |
| 27 | `IfNeedToSendSnapShotCommand` | 超阈值则 `Raft::Snapshot` → `Persister::Save` |
| 28 | `SendMessageToWaitChan` | `waitApplyCh[raftIndex]->Push(op)` ← **唤醒第 14 步** |

#### 阶段 6：应答

| # | 模块 | 动作 |
|---|---|---|
| 29 | `KvServer::PutAppend(2参数)` | 校验 `raftCommitOp` 与请求一致 → `reply->set_err(OK)` |
| 30 | `KvServer::PutAppend(5参数)` | 返回 → `done->Run()` |
| 31 | `RpcProvider::SendRpcResponse` | `SerializeToString` → `conn->send` |
| 32 | `Clerk::PutAppend` | `err()==OK` → `m_recentLeaderId = server` → 返回 |

#### 时序图

```text
Clerk        RPC层         KvServer        Raft        多数派Follower    Persister
  │            │              │              │               │              │
  ├─PutAppend─▶│              │              │               │              │
  │            ├─CallMethod──▶│              │               │              │
  │            │              ├─Start(op)───▶│               │              │
  │            │              │              ├──persist──────────────────────▶│
  │            │              │◀─raftIndex───┤               │              │
  │            │              │ 挂号 + 阻塞   │               │              │
  │            │              │              ├──AppendEntries─▶│              │
  │            │              │              │◀───success─────┤              │
  │            │              │              │ (多数派 → commit)             │
  │            │              │◀─applyChan───┤               │              │
  │            │              │ 去重 + 执行   │               │              │
  │            │◀─reply OK────┤ (唤醒)        │               │              │
  │◀───────────┤              │              │               │              │
```

### 三个容易讲错的边界

1. **`raftIndex` 是上下两半环唯一的汇合键**：`Start` 返回它，`Raft` 后来在 `ApplyMsg.CommandIndex` 报回同一个值。
2. **`Persister` 被两层共享，但写不同文件**：高频路径只重写 raftstate；快照同时写两份。
3. **阻塞发生在 RPC 工作线程里**：`timeOutPop` 阻塞的是 muduo 线程池中的一个线程，而同一个池也在处理 `AppendEntries`。

---

## Q4：三节点部署怎么做？

### 结论

| 问题 | 实际情况 |
|---|---|
| 部署形态 | **单机多进程**。`raftCoreRun` 用 `fork()` 起 n 个 `KvServer`，没有单节点入口 |
| 多机是否支持 | **不支持**——不是改配置能解决的，需要改代码 |
| 端口 | 启动器随机选 `startPort ∈ [10000,29999]`，节点 i 用 `startPort + i`（连号） |
| 配置文件 | 当前工作目录下的 `test.conf`，**启动时清空**，各节点启动后追加自己的 `ip/port` |
| 数据目录 | **没有目录**。`raftstatePersist<i>.txt`、`snapshotPersist<i>.txt` 直接落在 CWD |

### 进程模型

```text
raftCoreRun (父进程, main)
  ├─ fork → KvServer(0, 500, test.conf, P0)
  ├─ fork → KvServer(1, 500, test.conf, P1)
  ├─ fork → KvServer(2, 500, test.conf, P2)
  └─ pause()
```

- `fork()` 之间父进程 `sleep(1)`，节点串行拉起。
- 每个子进程各跑一个 `muduo::EventLoop`、一个 `IOManager`、一个 `applierTicker`。
- 没有"单节点启动"的可执行文件。

启动时序是硬耦合的（`kvServer.cpp:399-425`）：

```text
每个 KvServer 构造：
  sleep(6)                     // 等所有节点的 RpcProvider 起来
  读 test.conf 建 peers
  sleep(nodeCount - me)        // 等所有人都连完
  Raft::init()
```

**三个节点必须一起启动。**

### 端口

```cpp
std::uniform_int_distribution<> dis(10000, 29999);
unsigned short startPort = dis(gen);
short port = startPort + static_cast<short>(i);
```

风险：

| 风险 | 说明 |
|---|---|
| 端口冲突 | 随机落在已占用端口上时绑定失败；没有重试或换端口逻辑 |
| 无法固定端口 | 每次重启端口都会变 |
| 不能跨机 | `startPort+i` 只适合同一台机器 |

### 配置

```text
启动器 raftCoreRun
  ① 清空 test.conf（trunc）
  ② fork 节点 0..n-1
       └─ RpcProvider::Run：gethostname + gethostbyname 得到 IP
                          追加 node<i>ip=... / node<i>port=... 到 CWD 下的 test.conf
  ③ 每个 KvServer sleep(6) 后读 test.conf 建 peers
  ④ 客户端 Clerk::Init 读同一份文件
```

四个必须知道的约束：

1. **文件名硬编码为 `test.conf`**（`rpcprovider.cpp:65`），不是 `-f` 指定的路径。用 `-f other.conf` 会导致写和读不是同一份。
2. **强依赖 cwd**：读写都用相对路径，所以集群、`bench`、`callerMain` 都必须在 `bin/` 下启动。
3. **IP 自动探测且取最后一个地址**：多网卡机器可能选错接口；没有配置项可覆盖。
4. **追加写入不幂等**：节点重启一次文件就多一组 key；压测脚本靠启动前 `rm -f test.conf` 规避。

### 数据目录

| 文件 | 写入方 | 内容 |
|---|---|---|
| `test.conf` | `RpcProvider::Run` + 启动器 | 集群成员表 |
| `raftstatePersist<i>.txt` | `Raft` | term / votedFor / 日志 / 快照点 |
| `snapshotPersist<i>.txt` | `KvServer` | 跳表全量 + 去重表 |

文件名只按节点编号区分，没有集群名。后果：同目录跑两组集群会互相覆盖；清理状态就是 `rm -f raftstatePersist*.txt snapshotPersist*.txt`。

### 改成多机需要动什么

| # | 问题 | 改动 |
|---|---|---|
| 1 | 没有单节点入口 | 新增 `raftNode -i <id> -f <conf>`，不 fork |
| 2 | 启动器会 truncate 配置 | 配置改为只读；不再由节点回写 |
| 3 | `test.conf` 硬编码 | `RpcProvider::Run` 接受配置路径参数 |
| 4 | IP 自动探测 | 增加 `--advertise-ip`；绑定指定网卡 |
| 5 | 端口随机 | 配置中固定 + 启动时校验 |
| 6 | 数据文件在 CWD | 引入数据目录参数 |
| 7 | `sleep` 时序 | 改为显式握手/重试 |
| 8 | 初始化前发布服务 | 先 `init` 再 `NotifyService` |

第 7、8 条是真正门槛：现在的启动时序假设所有进程在同一台机器上、按固定间隔 fork。

### 实际跑一遍

```bash
mkdir -p cmake-build-debug && cmake -S . -B cmake-build-debug && cmake --build cmake-build-debug -j

cd bin
rm -f test.conf raftstatePersist*.txt snapshotPersist*.txt
./raftCoreRun -n 3 -f test.conf

# 另开终端，同样在 bin/ 下
cd bin && ./bench -t 2 -n 2000 -m put
```

---

## Q5：能力边界——论文实现、工程增强与生产缺口

### 一句话定位

**它是"共识算法 + 最小可用 KV"的完整闭环，不是"分布式存储系统"。**
算法层面达到论文要求；系统层面处于"研究原型"阶段。

### 能做的 / 不能做的

| 能做 | 不能做 |
|---|---|
| 多副本复制，容忍少数派故障 | 多机部署 |
| 线性一致读写（Get/Put/Append） | 动态增减节点 |
| 领导者选举 | 大 value（RPC 无分帧，约 1KB 上限） |
| 崩溃恢复（raftstate + snapshot 双层） | 事务 / 范围查询 / watch / lease |
| 日志压缩与远端快照安装 | TLS / 认证 / 鉴权 |
| 客户端幂等去重 | metrics / tracing / 结构化日志 |
| 单机多进程部署 | 优雅停机 / 在线备份 |
| 压测与故障注入 | 强耐久（无 fsync / 原子写） |

### 论文实现

| 论文条目 | 本项目 | 评价 |
|---|---|---|
| 持久化状态：`currentTerm` / `votedFor` / log | ✅ | 先落盘（flush 级）再应答 |
| `RequestVote`：term 三分支、`UpToDate`、`votedFor` | ✅ | 完整 |
| `AppendEntries`：`prevLogIndex/prevLogTerm` 检查 | ✅ | 完整 |
| 「同 index 同 term 则前缀相同」的写入语义 | ⚠️ | **覆盖而不截断尾部**，与伪码有偏差 |
| 随机化选举超时 | ✅ | 300–500 ms |
| Leader 只提交当前 term 的条目（§5.4.2） | ✅ | 注释写出了完整推理 |
| `commitIndex = min(leaderCommit, lastLogIndex)` | ✅ | 完整 |
| 快照与 `InstallSnapshot`（§7） | ✅ | 双向都实现了 |
| 客户端 exactly-once（§8） | ✅ | `clientId + requestId` + 快照持久化 |
| 冲突回退优化（§5.3） | ✅ | 用「回扫 term 变化点」实现 |

### 工程增强（论文没规定）

| 增强 | 作用 |
|---|---|
| 自研 mprpc（muduo + protobuf generic service） | 传输层，与共识解耦 |
| `ApplyMsg` + `LockQueue` 应用管道 | Raft ↔ 状态机单向解耦 |
| `waitApplyCh[raftIndex]` | 把异步 apply 转成同步 RPC 语义 |
| 双坐标日志 + 虚拟快照日志点 | 处理快照裁剪后的日志匹配 |
| raftstate / snapshot 双文件分离 | 高频路径不重写快照 |
| 状态机定义快照格式 | Raft 只搬运字节 |
| 自适应睡眠定时器 | 心跳续命、避免空转 |
| `ErrWrongLeader` + 环形重试 | leader 发现 |
| 协程调度 + 独立发送线程 | 避免慢节点阻塞主循环 |
| 压测/故障注入/持久化探针 | 验证手段 |

### 生产可用缺什么

#### T0：正确性与耐久性（不补不能用）

| # | 缺口 | 后果 |
|---|---|---|
| 1 | 持久化非原子、无 `fsync` | 崩溃窗口内状态全丢 |
| 2 | 单节点重启崩溃 | 重启即段错误 |
| 3 | 断言即 `exit` | 一个正常场景导致节点消失 |
| 4 | `CondInstallSnapshot` 未实现 | 快照无条件覆盖 |
| 5 | 日志尾部不截断 | 旧任期残余条目影响选举判断 |
| 6 | 数据竞争 | `m_status` 无锁读，UB |

#### T1：可用性与部署

单节点入口、配置外置、显式端口/IP、独立数据目录、跨机启动握手、PreVote/CheckQuorum、动态成员变更、优雅停机、备份恢复、TLS/鉴权、资源限制。

#### T2：性能与可观测性

批量/流水线、增量 WAL、快照流式传输与限流、ReadIndex/租约读、异步 RPC 处理、metrics/tracing。

#### T3：工程化与测试

确定性模拟器、线性一致性检查（Jepsen/Porcupine）、混沌测试与长跑、TSAN/ASAN、CI/CD、API 版本化、部署文档。

### 如果只能做三件事

1. **持久化改成 WAL + `fsync` + 原子 rename**
2. **修生命周期 + 移除 `exit` 断言**
3. **建确定性故障注入测试**

---

## Q6：重做一遍先改哪个设计？为什么？

### 直接回答：先改**持久化模型**

把"全量序列化 + truncate 重写"改成**追加式 WAL + 原子快照 + 恢复重放**。

理由不是它最"酷"，而是它是**唯一一个同时决定三件事**的设计：系统会不会丢数据、性能天花板在哪、崩溃后恢复到什么状态。

### 为什么是它

**1. 它是这个系统存在的理由**

现在的实现：

```cpp
void Persister::SaveRaftState(const std::string& data) {
  clearRaftState();             // truncate，文件瞬间变空
  m_raftStateOutStream << data;
  m_raftStateOutStream.flush(); // 只到 page cache
}
```

扛得住 `SIGKILL`，扛不住断电；在 truncate 到写完之间崩溃，文件是空的或残缺的。

**2. 它同时消除性能天花板**

| 成本项 | 复杂度 | 频率 |
|---|---|---|
| `persist()` 重写全部日志 | O(日志长度) | 每条命令 |
| `MakeSnapShot()` 转储整个跳表 | O(状态机大小) | 每 `maxraftstate/10` 字节 |

WAL 把第一项从 O(N) 降到 O(1) 追加。`benchmark/README.md` 已证明：**不存在能同时避开两者的 `maxraftstate` 取值**。

**3. 它把"两个文件的一致性"变成"一个 manifest 的原子切换"**

崩溃恢复变成确定性的：加载最新完整快照 → 从 manifest 指定位置重放 WAL → 校验并截断最后一个不完整记录。

**4. 它是更高层优化的前置条件**

批量写、group commit、pipeline、快照流式传输——全都要先有"追加日志"这个原语。

**5. 它让"崩溃恢复"变得可测试**

恢复逻辑是"重放到最后一个完整记录"，可以直接用**在任意字节偏移处截断文件**来测。

### 会改成什么样

```text
写入路径：
  Start(op) → 追加一条 WAL 记录 → 返回 index
                ↓
        [length][crc32][term][index][type][payload]
                ↓
        按策略 fsync（应答前置 / group commit）

恢复路径：
  1. 读 MANIFEST → 得到 snapshotIndex
  2. 加载 snapshot-<snapshotIndex>.dat
  3. 从 WAL 重放 index > snapshotIndex 的记录
  4. 遇到 CRC 失败或长度不完整 → 截断该位置之后的内容
  5. 重建 in-memory log index（offset map）与 matchIndex 基线

压缩路径：
  状态机做快照 → 写 snapshot-<index>.dat → fsync
             → 原子更新 MANIFEST → 删除 index 之前的 WAL 段
```

关键点：

- **日志在内存里只存索引**（offset → `(term, length)`），命中时按 offset 读文件；内存占用从 O(日志) 降到 O(条目数)。
- **快照不可变**，用编号命名，靠 MANIFEST 指针切换。
- **`persist()` 的语义从"重写状态"变成"追加一条记录"**。

### 为什么不是先做别的

| 候选 | 为什么排后面 |
|---|---|
| **PreVote** | 可用性问题；持久化不修，轮不到它起作用 |
| **动态成员变更** | 功能扩展；当前连单机重启都不可靠 |
| **ReadIndex / 租约读** | 纯性能优化 |
| **先建测试框架** | 方向对，但和 WAL 是同一批交付物：没有 WAL，崩溃恢复没有可测对象；WAL 的验收标准就是"任意字节截断后恢复出的日志是已提交前缀" |
| **修单节点重启崩溃** | 半天能修，但是 bug 不是设计；会在"生命周期"阶段顺带解决 |

### 重做顺序

| 阶段 | 内容 | 验收标准 |
|---|---|---|
| **1** | WAL + 原子快照 + 恢复重放 | 任意字节处截断 WAL，恢复出的日志是已提交前缀 |
| **2** | 核心状态所有权收归单线程（事件循环） | 消除 `m_status` 无锁读、detached 线程生命周期、锁顺序问题 |
| **3** | 生命周期与错误处理 | 单节点重启不崩；不变量违反时不 `exit` |
| **4** | 确定性模拟器 | 固定种子可复现；覆盖选举、分区、崩溃、重放 |
| **5** | 批处理 + ReadIndex | 延迟不再被 25 ms 心跳锁死；读不写日志 |
| **6** | 成员变更 / PreVote / TLS / metrics | 可运维、可扩展 |

第 1、2 步顺序有讲究：**先定持久化的语义，再定谁来驱动它**。反过来做，单线程核心会围绕一个错误的数据模型长出来，之后还得再拆一次。

### 一句话

> 我会先重做持久化，因为**它决定"系统能不能被信任"**。换成 WAL + 原子快照之后，耐久性、崩溃一致性、吞吐上限、可测试性会**同时**变好，而其他任何单点改动都做不到这一点。
>
> 这套分层、`ApplyMsg` 边界、双坐标日志模型、客户端幂等设计我会**保留**——它们是对的。要重做的不是架构，是**存储原语**。

---

## 附：关键数字速查

| 参数 | 值 | 来源 |
|---|---|---|
| 心跳间隔 | 25 ms | `config.h:11` |
| 选举超时（随机） | 300–500 ms | `config.h:14-15` |
| 客户端等待提交超时 | 500 ms | `config.h:17` |
| apply 间隔 | 10 ms | `config.h:12` |
| 快照阈值 | `m_maxRaftState / 10.0` = 50 字节 | `kvServer.cpp:343` |
| 默认节点数 / maxraftstate | 3 / 500 | `raftKvDB.cpp:58` |
| RPC 工作线程数 | 4 | `rpcprovider.cpp:91` |

---
---

# 第二部分：完整面试题库

> 标记说明：
> ✅ 已实现且有代码依据 ｜ ⚠️ 已实现但有明显局限 ｜ ❌ 未实现 / 未验证 ｜ ❓ 题目前提与代码不符
>
> **重要原则**：本项目很多能力**没有实现**。面试中如实说"没做，但我知道该怎么做/代价是什么"，比编造更有价值。

---

## 二、Raft 共识核心

### 选举

#### 2.1 随机化超时为什么取 300–500 ms？区间怎么定？节点数、RTT 变化怎么调？

**现状**：硬编码在 `config.h:14-15`，随 `debugMul` 整体放大；没有自适应。

**取值依据**（不是公式推出来的，是工程经验）：

- 约束一：`ElectionTimeout ≫ 心跳间隔 + 一次 RPC 的 RTT + 处理时间`，否则网络抖动会被误判成节点失联。
- 约束二：`ElectionTimeout` 不能太长，否则 leader 崩溃后恢复时间不可接受。
- 经验区间：`[10 × 心跳, 20 × 心跳]`。本项目心跳 25 ms，选举 300–500 ms，比值 12–20×，落在区间内。
- 随机跨度要足够大（这里 200 ms 的抖动），才能让同时超时的节点在**第一轮**就分出胜负，而不是反复平票。

**节点数与 RTT 变化时怎么调**：

| 场景 | 调整方向 |
|---|---|
| 节点数增加 | 广播一轮 RPC 的最坏耗时上升，选举超时要相应放大；随机区间也要放大，否则平票概率上升 |
| RTT 变大（跨机房） | 心跳间隔至少取 RTT 的 2–3 倍；选举超时取心跳的 10–20 倍 |
| 网络抖动大 | 用 P99 RTT 而不是平均 RTT 来定心跳 |

**更正确的做法**：把"心跳间隔"和"选举超时"从 RTT 的在线测量中推导，并加上滞回（hysteresis）避免抖动。本项目没有做。

#### 2.2 选举超时和心跳周期什么关系？心跳怎么取值，为什么？

**现状**：心跳 25 ms（`config.h:11`），选举超时 300–500 ms，比值约 12–20×。

**关系**：心跳的作用是**持续重置 follower 的选举计时器**。只要心跳间隔显著小于选举超时，丢一两个心跳不会触发选举。

**为什么取 25 ms**：

- 这是本机/局域网集群，RTT 通常 <1 ms，25 ms 留了足够余量；
- 更小（如 5 ms）会显著增加 CPU 和网络开销——**在本项目里尤其糟糕**，因为 `AppendEntries1` 的 `DEFER persist()` 让每个心跳都触发一次全量日志落盘；
- 更大（如 100 ms）会让写延迟更差，因为 `Start` 后的日志要等下一次心跳才复制。

**真实约束**：本项目写路径的延迟几乎等于"心跳间隔的一半 + 复制 + apply"，所以 25 ms 直接决定了 P50 ≈ 20.5 ms（见 Q8.2）。

#### 2.3 活锁（split vote）怎么产生？"自适应心跳"具体自适应什么？会引入不稳定吗？

**❓ 前提需要纠正**：本项目**没有"自适应心跳"**。

代码里的"自适应"指的是 `electionTimeOutTicker` 的**自适应睡眠**（`raft.cpp:325-345`）：

```cpp
suitableSleepTime = getRandomizedElectionTimeout() + m_lastResetElectionTime - wakeTime;
usleep(suitableSleepTime);
if (m_lastResetElectionTime - wakeTime > 0) continue;  // 睡眠期间被重置 → 不算超时
doElection();
```

它自适应的是"这次睡到什么时候"，**不是**改变心跳周期，也不是改变选举超时长度。

**split vote 的产生**：

1. 多个 follower 同时超时，几乎同时转为 Candidate；
2. 各自 term+1、投自己，并广播 RequestVote；
3. 每个节点只投一票，票被瓜分，没人拿到多数派；
4. 这一轮失败，各自超时后进入下一轮——如果随机化不够，会重复死锁。

**随机化为什么能解决**：让每个节点的超时起点错开，下一轮通常只有一个节点先醒来，先拿到多数票。

**如果真做"自适应心跳"**（动态调整心跳频率），风险是：

- 心跳变快 → CPU/网络开销上升，且本项目每个心跳都触发 persist，会放大磁盘写；
- 心跳变慢 → 选举误触发；
- 更危险的是**振荡**：节点根据瞬时负载调整，多个节点相互影响，可能进入"心跳变慢 → 误选举 → 再调快"的循环。

更稳妥的做法是自适应**选举超时**，心跳保持固定且远小于选举超时。

#### 2.4 RequestVote 的完整校验顺序？为什么这么排？

代码顺序（`raft.cpp` 的 `RequestVote` 2 参数版）：

```text
① args.term < currentTerm  → 拒绝（VoteState=Expire），直接 return，不重置计时器
② args.term > currentTerm  → 退位为 Follower，currentTerm = args.term，votedFor = -1
③ assert(args.term == currentTerm)
④ !UpToDate(lastLogIndex, lastLogTerm) → 拒绝（VoteState=Voted），return
⑤ votedFor != -1 && votedFor != candidateId → 拒绝（VoteState=Voted），return
⑥ 授票：votedFor = candidateId; lastResetElectionTime = now(); voteGranted = true
```

**为什么是这个顺序**：

1. **term 必须第一**：任期更小的候选人的请求一律无效，不能让它影响本地状态。
2. **term 更大时先退位再判断**：先成为 Follower、更新 term，才能用新 term 做后续判断。
3. **日志新旧在"是否已投"之前**：日志不够新的候选人无论投没投过都应被拒；而且这个拒绝不需要触碰 `votedFor`，逻辑更干净。
4. **`votedFor == candidateId` 允许重复授票**：RPC 重传时同一候选人可以再次拿到票（幂等）。
5. **只在真正授票时重置选举计时器**：拒绝投票不重置，否则一个不断被拒的候选人会无限压制其他节点选举。

> 论文里的顺序是 term → votedFor → log；本项目是 term → log → votedFor。两者在正确性上等价，因为任一拒绝分支都会直接返回，不会进入授票。

#### 2.5 候选者收到更大 term 如何退位？正在发送的 RPC 怎么处理？投票记录如何清理？

**退位**（`sendRequestVote` 的回复处理）：

```cpp
if (reply->term() > m_currentTerm) {
    m_status = Follower;
    m_currentTerm = reply->term();
    m_votedFor = -1;      // 新任期尚未投票
    persist();
    return;
}
```

`votedFor = -1` 表示"新任期还没投过票"，**不是**清空历史记录——旧任期的投票信息已经无意义。

**正在发送的 RPC**：**不主动取消**。本项目没有请求取消机制，detached 线程会继续跑完。它们的回复会被 term 检查丢弃：

```cpp
if (reply->term() < m_currentTerm) return;  // 旧回复，忽略
```

这在正确性上是安全的，因为 Raft 的 RPC 都是幂等的；代价是浪费带宽和线程。

**其他退位路径**：

- `AppendEntries1` 收到更大 term → 退位并更新 term；
- `InstallSnapshot` 收到更大 term → 同上；
- `leaderSendSnapShot` 收到更大 term → 同上。

#### 2.6 PreVote / Leader Lease 实现了吗？没有的话分区下会怎样？

**❌ 都没实现。**

**没有 PreVote 的后果（term 膨胀 / 打断）**：

1. 一个节点被网络分区，收不到心跳，反复超时竞选，`currentTerm` 一路增大；
2. 分区恢复后，它带着一个很大的 term 发 RequestVote；
3. 正常 leader 收到后必须退位并把 term 更新到更大值；
4. 集群被迫重新选举——即使这个回归节点根本没有足够新的日志，也当不上 leader。

结果：**一次分区恢复会打断正在服务的集群，产生一段 300–500 ms 的不可用**。

PreVote 的做法是：先发一轮"试探性投票"，只有确认自己**能拿到多数派**才真正增加 term、发起选举，从而避免无谓的 term 膨胀。

**没有 Leader Lease 的后果**：

- 本项目**读也走完整共识**，所以不存在"lease 过期后读到旧值"的问题——它是用性能换正确性；
- 代价是每个 GET 都要写一条日志、复制到多数派、落盘，读吞吐和写吞吐同一量级。

#### 2.7 3 节点分成 1+2，各会发生什么？恢复后以哪边为准？旧 Leader 不会继续写？

**分两种情况**：

**A. Leader 在少数派（1 侧）**

- 它仍然认为自己是 leader，`Start` 会成功追加日志并返回 index；
- 但它**无法提交**（拿不到多数派），`commitIndex` 不动，`applyChan` 不推消息；
- 客户端等待 500 ms（`CONSENSUS_TIMEOUT`）后超时，收到 `ErrWrongLeader`，换节点重试；
- 它的 `currentTerm` 因为收不到更高 term 的消息而保持不变。

**B. 多数派（2 侧）**

- 它们收不到心跳，选举计时器超时，其中一个当选新 leader（term+1）；
- 新 leader 可以正常提交（2/3 是多数派）；
- 客户端重试时会打到新 leader。

**恢复后以哪边为准**：**多数派**。已提交的日志只可能在多数派上，且 Raft 的 Leader Completeness 保证新 leader 拥有全部已提交日志。

**旧 Leader 会不会继续写**：

- 不能提交，所以**不会产生已确认的写**；
- 但它本地 `m_logs` 里可能留下未提交的条目。恢复后新 leader 的 `AppendEntries` 会用 `prevLogIndex/prevLogTerm` 让这些条目被覆盖（本实现是**就地覆盖 + 追加，不截断尾部**，见 2.13）；
- 如果旧 leader 的 term 更高（它没停止竞选的话），它恢复后可能触发一轮新选举——这是没有 PreVote 的典型代价。

#### 2.8 选举期间到达的客户端请求怎么处理？丢弃、排队还是 NotLeader？为什么？

**现状：直接返回 `ErrWrongLeader`，不排队、不丢弃。**

```cpp
m_raftNode->Start(op, &raftIndex, &_, &isleader);
if (!isleader) { reply->set_err(ErrWrongLeader); return; }
```

客户端 `Clerk` 收到后换下一个节点重试（环形），`requestId` 不变。

**为什么这样设计**：

- 无状态：服务端不维护排队队列，不需要处理队列满、超时、节点切换后的迁移；
- 简单且安全：客户端重试 + 服务端去重是标准做法；
- 代价：选举窗口内客户端会经历多轮失败重试，延迟明显上升；如果客户端没有退避，会产生大量无效 RPC。

**一个边界**：如果节点是 leader 但拿不到多数派，`Start` 会成功、请求会阻塞到超时，而不是立即返回 `ErrWrongLeader`——这是延迟更长的一条路径。

### 日志复制与提交

#### 2.9 AppendEntries 的完整流程与所有失败分支？

**入口**：`Raft::AppendEntries(5参数)` → `AppendEntries1(2参数)`。

```text
1. args.term < currentTerm
   → success=false, term=currentTerm, UpdateNextIndex=-100, return
   （注意：不重置选举计时器）

2. args.term > currentTerm
   → status=Follower, currentTerm=args.term, votedFor=-1

3. status=Follower
   m_lastResetElectionTime = now()      // 心跳续命
   DEFER { persist(); }                 // 出作用域落盘

4. prevLogIndex > lastLogIndex
   → success=false, UpdateNextIndex=lastLogIndex+1, return       // follower 太短

5. prevLogIndex < lastSnapshotIncludeIndex
   → success=false, UpdateNextIndex=snapshotIdx+1, return        // follower 太快，落在快照里
   （★ 必须 return，否则 matchLog 的断言会 exit 进程）

6. matchLog(prevLogIndex, prevLogTerm) 失败
   → 回扫：从 prevLogIndex 往前找第一个 term 不同的位置
   → UpdateNextIndex = 该位置 + 1, success=false, return

7. 匹配成功
   → 逐条处理 entries：比 lastLogIndex 大则 push_back，否则按 term 不一致就地覆盖
   → commitIndex = min(leaderCommit, lastLogIndex)
   → success=true, term=currentTerm
```

**失败分支的三种回退值**：

| 分支 | UpdateNextIndex |
|---|---|
| term 过期 | `-100`（哨兵，leader 不更新 nextIndex） |
| follower 日志太短 | `lastLogIndex + 1` |
| follower 快照更新 | `snapshotIdx + 1` |
| prevLogTerm 不匹配 | 回扫得到的 term 变化点 + 1 |

#### 2.10 回扫算法、复杂度、最坏情况、会不会读到已裁剪日志？

**算法**（`AppendEntries1` 的 else 分支）：

```cpp
reply->set_updatenextindex(args->prevlogindex());
for (int index = args->prevlogindex(); index >= m_lastSnapshotIncludeIndex; --index) {
    if (getLogTermFromLogIndex(index) != getLogTermFromLogIndex(args->prevlogindex())) {
        reply->set_updatenextindex(index + 1);
        break;
    }
}
```

从 `prevLogIndex` 向前，找到第一个 term 与 `prevLogTerm` 不同的位置，返回它 +1。这利用了"日志中的 term 单调不减"这一性质：连续一段相同 term 的条目，要么全匹配，要么全冲突，所以可以整段跳过。

**复杂度**：`O(k)`，k = `prevLogIndex` 所在 term 段的长度。最坏是 `O(日志长度)`（整个日志同一 term），但实际冲突通常只涉及 1–2 个条目，回扫 0–1 次就停。

**和逐条回退的对比**：

- 论文的朴素做法：`nextIndex--`，每轮 RPC 只退一格 → 最坏 `O(日志长度)` 轮 RPC；
- 本项目：一轮 RPC 内完成回扫 → 最坏 1 轮 RPC + `O(日志长度)` 的 CPU；
- 用**带宽换 CPU**，在日志很长时优势明显。

**会不会读到已裁剪日志**：不会。

- 循环下界是 `index >= m_lastSnapshotIncludeIndex`；
- `getLogTermFromLogIndex` 对 `index == snapshotIdx` 有特判，返回快照 term；
- 如果 `prevLogIndex < snapshotIdx`，在第 5 步就已经返回了，根本不会进入回扫。

#### 2.11 commitIndex 如何推进？matchIndex 怎么维护？多数派怎么算？

**匹配数组的维护**（`sendAppendEntries` 的回复处理）：

```cpp
m_matchIndex[server] = std::max(m_matchIndex[server], args->prevlogindex() + args->entries_size());
m_nextIndex[server]  = m_matchIndex[server] + 1;
```

- 用 `max` 而不是直接赋值：心跳会重复发送同一批日志，直接赋值会让 `matchIndex` 被旧回复拉低/重复计数；
- 失败（`!success`）时只更新 `nextIndex`，**不更新 `matchIndex`**；
- 新 leader 上任时：`nextIndex[i] = lastLogIndex + 1`，`matchIndex[i] = 0`。

**commitIndex 推进**：

```cpp
(*appendNums)++;
if (*appendNums >= 1 + m_peers.size() / 2) {        // 多数派
    if (entries非空 && entries.last.term == m_currentTerm) {   // 只提交当前 term
        m_commitIndex = std::max(m_commitIndex, prevLogIndex + entries.size());
    }
}
```

**多数派怎么算**：`appendNums` 在 `doHeartBeat` 里初始化为 **1**（把 leader 自己算一票），阈值是 `1 + n/2`。3 节点 → 需要 2；5 节点 → 需要 3。

**一个已知缺陷**：`m_matchIndex[m_me]` 恒为 0，leader 自己的进度只靠 `appendNums` 的初值 1 来代表。提交逻辑因此是正确的，但以后要加 ReadIndex / 租约读（都要求 leader 自己已匹配到 `lastLogIndex`）时会踩坑。

#### 2.12 为什么必须"仅提交当前任期的日志"？不遵守的反例（时序图）

**结论**：leader 不能仅凭"某条旧任期日志已经复制到多数派"就提交它。必须先提交一条**当前任期**的日志，由它"携带"之前所有条目一起提交。

**反例（论文 Figure 8）**：

```text
时间   S1(term2, leader)   S2          S3          S4          S5
────────────────────────────────────────────────────────────────────
t1     append idx2(T2) ──▶ 收到
t2     ✗ 崩溃（未提交）
t3                                        S5 当选 term3
                                          append idx2(T3)
t4                                        ✗ 崩溃
t5     S1 重启，当选 term4
       idx2(T2) 复制到 S3   （S1+S2+S3 = 多数派）
       ⚠️ 若此时仅凭多数派就提交 idx2(T2) ...
t6     S1 崩溃
t7     S5 当选 term5（S5 的 idx2 是 T3 > T2，日志"更新"，能拿到票）
       S5 用自己的 idx2(T3) 覆盖 → 已"提交"的 T2 条目被抹掉
```

**关键点**：S5 的日志在 idx2 上是更高的 term，按 `UpToDate` 规则它"更新"，所以它能当选。如果 S1 在 t5 提交了 T2 的条目，t7 就会发生**已提交的写被覆盖**。

**为什么当前任期的条目安全**：一旦当前任期的条目被多数派确认并提交，任何能当选的节点都必须包含它（Leader Completeness），而它的 index 之前的所有条目也随之确定。所以"提交当前任期条目"是一个安全的锚点。

#### 2.13 Leader 崩溃后，未提交日志如何被新 Leader 覆盖？Follower 上怎么实现？

**正确流程**：

1. 新 leader 上任后把 `nextIndex[i]` 重置为 `lastLogIndex+1`，`matchIndex[i]=0`；
2. 通过 `AppendEntries` 的 `prevLogIndex/prevLogTerm` 检查发现不匹配；
3. follower 返回回退位置，leader 逐轮回退 `nextIndex`；
4. 找到匹配点后，leader 从匹配点之后发送自己的日志；
5. follower 遇到冲突条目时**删除冲突条目及其后的所有条目**，再追加新条目。

**本项目的实现差异（⚠️）**：

```cpp
for (int i = 0; i < args->entries_size(); i++) {
    auto log = args->entries(i);
    if (log.logindex() > getLastLogIndex()) m_logs.push_back(log);      // 追加
    else if (m_logs[...].logterm() != log.logterm()) m_logs[...] = log; // 就地覆盖
}
```

- **只覆盖冲突位置，从不删除尾部条目**；
- 如果 follower 的日志比 leader 长（有旧任期的残余尾部），这些条目会保留下来，导致 follower 的日志不再是 leader 日志的前缀；
- 大部分情况下，leader 会从 `prev+1` 一直发到自己的末尾，窗口内的所有位置都被覆盖成 leader 的版本，效果等价于截断；但窗口**之外**的尾部不会清理。

**风险**：`lastLogIndex` 偏高会影响 `UpToDate` 判断，可能让带残余尾部的节点在选举中显得更"新"。这是与论文的实质偏差。

#### 2.14 日志空洞可能出现在哪里？如何保证连续？

**结论：本项目的正常路径不会产生空洞。**

保证来自三处：

1. **复制是连续的**：`AppendEntries` 从 `prevLogIndex+1` 开始，按顺序发送到 leader 末尾；follower 只会 `push_back` 或覆盖已有位置，不会跳过某个 index；
2. **裁剪只发生在前缀**：`Snapshot` 和 `InstallSnapshot` 都只删除 `[snapshotIdx, ...]` 之前的部分；
3. **断言兜底**：`getSlicesIndexFromLogIndex` 用算术换算，如果日志有洞，索引和内容会错位；代码里还有 `m_logs[i].logindex() == i` 的一致性检查（`getApplyLogs`）。

**潜在风险**：正因为索引是**算术推导**的（`vector[i]` 就假定 logIndex = snapshotIdx + i + 1），一旦某处写入破坏了连续性，不会立即报错，而是静默错位。这比显式存储 index 更脆弱。

#### 2.15 Apply 到状态机的时机？串行还是并行？如何保证只 Apply 一次？

**时机**：`applierTicker` 每 `ApplyInterval`（10 ms）检查一次：

```cpp
while (m_lastApplied < m_commitIndex) {
    m_lastApplied++;
    ApplyMsg{CommandValid=true, Command=..., CommandIndex=m_lastApplied};
}
applyChan->Push(msg);
```

**串行**：

- 生产端只有一个 `applierTicker` 线程；
- 消费端只有一个 `ReadRaftApplyCommandLoop` 线程；
- 中间是 FIFO 的 `LockQueue<ApplyMsg>`。

所以 apply 严格按 index 顺序、单线程执行。

**只 Apply 一次**：

- `m_lastApplied` 单调递增，不会重复推送同一个 index；
- 进程崩溃重启后 `m_lastApplied` 会重置（不持久化），已提交的日志会从快照点之后重放；
- 重放时的幂等由 `KvServer` 的去重表 `(clientId, requestId)` 保证；
- 同时 `message.CommandIndex <= m_lastSnapShotRaftLogIndex` 会跳过快照已覆盖的部分。

> 注意：这里的"只 Apply 一次"是**逻辑上只执行一次**；物理上日志可能被重放，靠去重表兜底。

### 状态机与 KV 语义

#### 2.16 命令格式？Put/Append/Get 如何编码？Get 走不走 Raft？

**命令格式**（`util.h` 的 `Op`）：

```cpp
class Op {
  std::string Operation;  // "Get" / "Put" / "Append"
  std::string Key;
  std::string Value;
  std::string ClientId;
  int RequestId = 0;
};
```

- `Op` 用 **boost 文本归档**序列化成字符串（`Op::asString()`）；
- 这个字符串被塞进 `LogEntry.Command`（protobuf 的 `bytes` 字段）；
- 即"boost 序列化嵌在 protobuf 里"。

**Get 走不走 Raft**：**走**。`KvServer::Get` 同样构造 `Op{Operation="Get"}` 并调用 `Raft::Start`，日志复制、提交、apply 之后才读取本地 `SkipList`。

**为什么**：这是不用 ReadIndex / LeaseRead 时保证线性一致读的最简单方式——读也进入全局日志顺序。代价是读延迟和写一样，且读也产生日志。

#### 2.17 线性一致读怎么保证？如果 Leader 时钟不准，LeaseRead 会怎样？

**本项目的做法**：读也走一遍共识（见 2.16）。只要 Raft 保证日志的线性一致，读就线性一致。

**没有用 ReadIndex / LeaseRead**：

- ReadIndex：先向多数派确认自己仍是 leader，拿到一个 read index，等本地状态机 apply 到该 index 后再读；
- LeaseRead：leader 认为在租约期内自己是唯一的 leader，直接本地读。

**如果实现了 LeaseRead 而时钟不准**：

- 租约依赖"时钟漂移有上界"的假设。如果 follower 的时钟慢、leader 的时钟快，leader 可能在自己的租约已过期时仍认为有效；
- 旧 leader 在分区后继续用本地状态响应读，**返回陈旧数据**——违反线性一致；
- 解决：只用单调时钟、租约时长取 `electionTimeout - 安全裕度`、要求时钟漂移有界，或者干脆用 ReadIndex 避免依赖时钟。

本项目因为是"读走日志"，**不受时钟问题影响**，这是它正确性上的一个优点。

#### 2.18 Append 的语义是什么？并发 Append 如何定序？

**⚠️ 重要发现：本项目没有真正实现 Append 语义。**

`ExecuteAppendOpOnKVDB` 调用的是：

```cpp
m_skipList.insert_set_element(op.Key, op.Value);
```

而 `insert_set_element` 的语义是"存在则删除后重新插入"，即**覆盖**：

```cpp
void SkipList::insert_set_element(K& key, V& value) {
    V oldValue;
    if (search_element(key, oldValue)) delete_element(key);
    insert_element(key, value);
}
```

函数里被注释掉的才是真正的追加逻辑：

```cpp
// if (m_kvDB.find(op.Key) != m_kvDB.end())
//     m_kvDB[op.Key] = m_kvDB[op.Key] + op.Value;
```

所以 **`Append` 实际等价于 `Put`**。这是一个语义缺陷，面试中可以作为"我知道它没实现"的点主动说明。

**并发 Append 定序**（如果实现正确）：所有操作都进入同一条 Raft 日志，按 log index 串行 apply，因此天然有全序。同一 key 的并发 Append 的最终结果由日志顺序决定。

#### 2.19 KV 用什么结构存？为什么跳表而不是 unordered_map / LSM？层数、概率？范围查询/迭代器？

**结构**：`SkipList<std::string, std::string>`，模板参数在 `KvServer` 里实例化为 `SkipList<std::string, std::string> m_skipList`。

**为什么跳表**：

| 对比 | 跳表 | unordered_map | LSM |
|---|---|---|---|
| 有序性 | ✅ 天然有序，可范围查询 | ❌ 无序 | ✅ |
| 实现复杂度 | 低 | 低 | 高 |
| 写性能 | O(log n) 内存写 | O(1) | 写放大/compaction 复杂 |
| 持久化 | 需要整体 dump/load | 同样 | 天然按层落盘 |
| 适用场景 | 内存有序索引 | 纯 KV | 大容量、写多 |

选跳表的核心理由：**有序**（为范围查询留空间）+ **实现简单**（适合教学项目）+ Redis 同款设计。

**层数与概率**：

```cpp
int SkipList::get_random_level() {
  int k = 1;
  while (rand() % 2) k++;      // 概率 0.5
  k = (k < _max_level) ? k : _max_level;
  return k;
}
```

- 最大层数由构造参数决定，`KvServer` 传的是 **6**（`m_skipList(6)`）；
- 提升概率 **0.5**（常见实现用 0.25，层数会更少、内存更省）；
- 注意：使用的是 `rand()`，**没有 `srand()`**——这和 clientId 的 bug 同源。

**范围查询/迭代器**：**❌ 未实现**。跳表有 level-0 链表，理论上可以顺序遍历；`dump_file()` 就是沿 level-0 遍历全部节点。但 RPC 层只暴露了单 key 的 Get/Put/Append，没有 range API。

#### 2.20 跳表并发控制？压测时锁竞争是不是瓶颈？

**现状**：

- 跳表内部有 `std::mutex _mtx`，在 `insert_element`/`delete_element` 里加锁；
- `search_element` **不加锁**；
- 但所有读写实际上都在**同一个 apply 线程**里发生，`KvServer` 另外还有一把 `m_mtx`。

所以实际是**嵌套锁**：`KvServer::m_mtx`（在 `ExecuteXxx` 里）→ `SkipList::_mtx`。锁顺序固定，没有反向获取，不存在死锁。

**锁竞争是不是瓶颈**：**不是**。因为只有一个 apply 线程，锁基本无竞争。真正的瓶颈是心跳驱动的复制延迟、全量持久化和快照（见第八节）。

**改进方向**：如果要做并发读，可以改成读写锁/分片锁；但前提是先把 apply 与实际读分离（目前读也要等 apply）。更彻底的做法是 copy-on-write 或 MVCC。

---

## 三、双坐标日志模型与快照

#### 3.1 双坐标具体指什么？为什么需要两套？换算函数与边界条件？

| 坐标 | 含义 | 起点 |
|---|---|---|
| **logIndex** | Raft 逻辑下标：`LogEntry.logindex()`、`prevLogIndex`、`commitIndex`、`ApplyMsg.CommandIndex` | 1（快照后从 `snapshotIdx+1` 起） |
| **sliceIndex** | `std::vector<LogEntry>` 的物理下标 | 0 |

**为什么需要两套**：快照会从日志头部裁掉一段（`m_logs` 只保留 `snapshotIdx` 之后的条目），于是 `vector[0]` 对应的 `logIndex` 不再是 1，而是 `snapshotIdx+1`。所有"用 logIndex 访问 vector"的地方都必须减去偏移。

**换算函数**：

```cpp
int Raft::getSlicesIndexFromLogIndex(int logIndex) {
  myAssert(logIndex > m_lastSnapshotIncludeIndex, ...);   // 严格大于
  myAssert(logIndex <= getLastLogIndex(), ...);
  return logIndex - m_lastSnapshotIncludeIndex - 1;
}
```

**边界条件**：

- `logIndex == snapshotIdx`：**不允许**传入（因为 vector 里没有这一条），所以快照点本身用 `getLogTermFromLogIndex` 的特判处理；
- `logIndex > lastLogIndex`：越界，断言触发（`exit`）。

#### 3.2 偏移量存在哪里？重启后如何恢复？

偏移量就是 `m_lastSnapshotIncludeIndex`（以及配套的 `m_lastSnapshotIncludeTerm`）。

- 写入：`Raft::Snapshot` 和 `InstallSnapshot` 会更新它们；
- 持久化：随 `BoostPersistRaftNode` 一起写入 raftstate；
- 恢复：`Raft::init` 先 `readPersist`，把这两个值读回来；如果 `snapshotIdx > 0`，还会把 `m_lastApplied = snapshotIdx`；
- 状态机：`KvServer` 构造时读 `Persister::ReadSnapshot()`，反序列化重建跳表。

所以 `firstIndex = snapshotIdx + 1` 是**推导出来的**，不需要单独存储。

#### 3.3 虚拟日志条目的 index/term 取什么？会不会和真实条目冲突？

- `index = m_lastSnapshotIncludeIndex`
- `term = m_lastSnapshotIncludeTerm`

它代表"快照覆盖的最后一条日志"。虽然这条日志的原文已经不在 `m_logs` 里，但它的 `(index, term)` 仍然参与匹配：

```cpp
int Raft::getLogTermFromLogIndex(int logIndex) {
  if (logIndex == m_lastSnapshotIncludeIndex) return m_lastSnapshotIncludeTerm;  // 虚拟条目
  return m_logs[getSlicesIndexFromLogIndex(logIndex)].logterm();
}
```

**不会冲突**：真实的 vector 下标只覆盖 `logIndex > snapshotIdx`，而虚拟条目只在 `logIndex == snapshotIdx` 时生效。两个函数的断言方向刻意不同（一个 `>`，一个 `>=`），把边界切得干净。

**用途**：

- `matchLog` 可以检查 `prevLogIndex == snapshotIdx` 的情况；
- `getPrevLogInfo` 在 `nextIndex == snapshotIdx+1` 时返回快照的 index/term 作为 prev；
- `doHeartBeat` 构造 entries 时特判 `preLogIndex == snapshotIdx`。

#### 3.4 Follower 需要的日志被 Leader 裁剪了怎么办？InstallSnapshot 的完整流程与分块？

**检测**：leader 的 `doHeartBeat` 里：

```cpp
if (m_nextIndex[i] <= m_lastSnapshotIncludeIndex) {
    std::thread t(&Raft::leaderSendSnapShot, this, i);
    continue;
}
```

**流程**：

```text
leader:
  读 persister 里的 snapshot 字节
  构造 InstallSnapshotRequest{leaderid, term,
      lastSnapShotIncludeIndex/Term, Data=snapshot}
  RPC → follower

follower (Raft::InstallSnapshot 2 参数):
  1. args.term < currentTerm → 回 term，返回
  2. args.term > currentTerm → 退位、更新 term、persist
  3. status = Follower；重置选举计时器
  4. args.lastSnapShotIncludeIndex <= m_lastSnapshotIncludeIndex → 过时，返回
  5. 截断日志：
       lastLogIndex > snapshotIdx → erase(begin, begin + sliceIdx(snapshotIdx) + 1)
       否则 m_logs.clear()
  6. commitIndex = max(commitIndex, snapshotIdx)
     lastApplied = max(lastApplied, snapshotIdx)
     lastSnapshotIncludeIndex/Term = args 里的值
  7. 构造 ApplyMsg{SnapshotValid=true, Snapshot=Data, SnapshotIndex, SnapshotTerm}
     → 起 detached 线程 push 到 applyChan
  8. Persister::Save(persistData(), args.data())
```

**分块传输：❌ 未实现。** 整个快照塞进一个 protobuf `bytes` 字段，一次 RPC 发送。问题：

- 快照大时会超过 RPC 的 1024 字节接收缓冲（见 6.2）；
- 单次传输失败要整体重传；
- 无法断点续传、无法限流。

工业实现会分块（chunk）+ 校验 + 断点续传。

#### 3.5 传输中途失败/崩溃，如何保证不产生半截状态？原子替换还是增量？

**RPC 层**：如果 RPC 失败，`ok=false`，leader 下一轮心跳会再次触发 `leaderSendSnapShot`（`nextIndex` 仍 `<= snapshotIdx`）。follower 只有完整收到 RPC 参数并反序列化成功后才会改状态。

**本地持久化层：⚠️ 不原子**：

```cpp
void Persister::Save(raftstate, snapshot) {
  clearRaftStateAndSnapshot();   // 两个文件先 truncate
  ... 写入 ...
  flush(); flush();              // 无 fsync
}
```

- 在 truncate 到写完之间崩溃 → 两个文件为空或残缺；
- 没有临时文件 + rename，所以**不是原子替换**；
- 没有 CRC/校验和，半截状态无法检测。

**正确做法**：写临时文件 → fsync → rename → fsync 目录；恢复时校验 manifest/CRC。

#### 3.6 快照内容包含什么？序列化格式？如何做版本兼容？

**内容**（`KvServer::serialize`）：

```cpp
ar & m_serializedKVData;   // SkipList::dump_file() 的结果
ar & m_lastRequestId;      // clientId → requestId 去重表
```

- `m_serializedKVData`：`SkipListDump<K,V>` 的 boost 文本归档；
- `m_lastRequestId`：`unordered_map<string,int>`。

**格式**：boost 文本归档（`text_oarchive`），**嵌套一层**——外层是 `KvServer` 的归档，里面 `m_serializedKVData` 又是跳表的 boost 归档。

**版本兼容：❌ 没有。** 没有版本号、没有字段标签、没有向后兼容策略。任何结构变化都会让旧快照无法解析。改进：加 `version` 字段 + 逐版本迁移函数。

#### 3.7 快照触发条件？快照期间新日志如何并发？一致性怎么保证？

**触发条件**（`IfNeedToSendSnapShotCommand`）：

```cpp
if (m_raftNode->GetRaftStateSize() > m_maxRaftState / 10.0) { ... }
```

**⚠️ 这里有两个 bug**（见 Q8）：
- `proportion` 参数传了 9 但没用；
- 阈值是 `maxraftstate/10`，`maxraftstate=500` → 50 字节，几乎每条命令都触发。

**快照期间的并发**：

- 快照是在 **apply 线程**上、处理完 index 之后做的；
- `MakeSnapShot` 持 `KvServer::m_mtx`，序列化当前跳表和去重表；
- 同时 Raft 的复制线程可能仍在追加**新日志**，但那些日志还没 apply，所以不影响快照内容；
- `Raft::Snapshot(index, snapshot)` 持 `Raft::m_mtx`，截断 `index` 之前的日志，并持久化。

**一致性**：快照的语义是"截止到 index 的状态机状态"。因为 apply 串行、快照在 apply 之后取，且 `Snapshot` 只丢弃 `index` 之前的日志，所以快照与日志是自洽的。

**风险**：`Raft::Snapshot` 持锁做全量序列化 + 文件写，会阻塞心跳处理。

#### 3.8 双向快照的"双向"指哪两个方向？和单向相比解决了什么？

| 方向 | 触发者 | 作用 |
|---|---|---|
| **本地产生** | 状态机（`KvServer`） | 日志太大时生成快照，让 Raft 截断日志，控制磁盘/内存 |
| **远端安装** | leader | follower 落后太多、所需日志已被 leader 裁剪时，把快照发过去让它追上 |

**只有单向会怎样**：

- 只有本地产生：leader 裁剪了日志后，落后的 follower 永远追不上（缺少缺失的日志）；
- 只有远端安装：本地日志无限增长，没有压缩。

两者合起来才形成完整的生命周期。

#### 3.9 快照与 compact 后旧日志/文件如何回收？磁盘空间如何管理？

**现状：没有独立日志文件，所以没有分段回收。**

- 所有日志都在 `raftstatePersist<i>.txt` 这一个文件里；
- `Snapshot` 截断内存中的 `m_logs` 后，`persistData()` 重新序列化剩余日志 → `Save()` **truncate 并重写**这个文件；
- 旧数据被覆盖，磁盘空间自然回收，但代价是每次快照/清理都重写全量。

**快照文件**：`snapshotPersist<i>.txt` 同样被整体覆盖，**只保留最新一份**，没有历史快照。

**问题**：

- 没有增量 WAL，写放大严重；
- 没有磁盘配额/水位线；
- 文件损坏后没有备份可回滚。

#### 3.10 两节点日志长度相同但内容不同（term 相同），匹配算法会怎样？

`matchLog` 只比较 `prevLogIndex` 处的 term：

```cpp
return logTerm == getLogTermFromLogIndex(logIndex);
```

如果 prev 处的 term 相同，就认为匹配，继续逐条写入。写入时对每个条目：

```cpp
if (同 index 且同 term && command 不同) {
    myAssert(false, "两节点 logIndex 和 term 相同，但 command 却不同！！");
}
```

**结论**：

- 同 index + 同 term + 同 command → 视为已匹配，跳过；
- 同 index + 同 term + 不同 command → **触发断言，进程 exit**；
- 同 index + 不同 term → 就地覆盖。

后者理论上不可能出现（Log Matching Property 保证同 index 同 term 的条目必然相同），一旦出现说明实现或数据损坏。这是项目里**唯一一处主动检查该不变量**的地方，但处理方式是杀进程，不是降级。

---

## 四、线性一致性与"恰好一次"语义

#### 4.1 去重表的结构？如何淘汰旧条目（无界增长）？

```cpp
std::unordered_map<std::string, int> m_lastRequestId;  // clientId → 最后执行的 requestId
```

判重：

```cpp
if (m_lastRequestId.find(clientId) == end) return false;
return requestId <= m_lastRequestId[clientId];
```

**淘汰：❌ 没有。** 表只增不减：

- 每个客户端一个条目；
- 条目随快照持久化，所以会一直存在；
- 长期运行下内存会缓慢增长（每条目约几十字节 + 字符串）。

**改进**：

- 按会话 TTL 淘汰（要求 TTL > 客户端最大重试窗口）；
- LRU/LFU（但要保证被淘汰的会话不再重试）；
- 分片 + 版本化快照。

#### 4.2 去重表随快照持久化——被裁剪后重试会不会重复执行？安全窗口？

**⚠️ 前提不成立**：当前实现**不会裁剪去重表**。快照里存的是整个 `m_lastRequestId`，所以每次快照都完整保留。

如果将来做了裁剪，语义会退化为：

- 客户端在条目被淘汰后重试旧请求 → 判重失效 → `Append` 被重复执行；
- 安全窗口 = 条目保留时长，必须 ≥ 客户端的最大重试时长（包括客户端崩溃后重启再重试的时间）；
- 超出窗口后只能靠"客户端不再重试旧请求"的应用层约定。

**当前的风险不在裁剪，而在 clientId 冲突**（见 9.1）：两个客户端共用同一个 clientId，导致去重表把新客户端当成旧客户端的重复请求。

#### 4.3 客户端重试时"ID 不变"怎么保证？客户端崩溃重启后如何恢复 requestId？

**重试不变**：`Clerk::PutAppend` 在**每次逻辑操作开始时** `m_requestId++`，把值存到局部变量 `requestId`；整个 `while(true)` 重试循环里都用这个局部变量。

**客户端重启**：

- `Clerk` 构造时生成新的 `clientId`（UUID）；
- `m_requestId` 从 0 重新开始；
- 因为 `clientId` 变了，即使 `requestId` 重复，也不会被误判为重复请求。

**所以不需要持久化 requestId**。前提是 clientId 真的唯一——原实现 `Uuid()` 用了 `rand()` 但没 `srand()`，导致跨进程 clientId 相同，这正是"静默丢写"的根因。

#### 4.4 raftIndex 等待队列：结构、并发、超时、日志被覆盖如何通知？

**结构**：

```cpp
std::unordered_map<int, LockQueue<Op>*> waitApplyCh;  // raftIndex → 队列
```

**并发**：`KvServer::m_mtx` 保护插入、查找、删除；`LockQueue` 内部有自己的 mutex + condition_variable。

**等待**：

```cpp
chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp);  // 500 ms
```

**超时处理**：

1. 返回 `false`；
2. 检查 `ifRequestDuplicate && 自己仍是 leader`，是则本地执行 Get 的快速路径；
3. 否则回 `ErrWrongLeader`；
4. 加锁，`waitApplyCh[raftIndex]` 取出、`erase`、`delete`。

**日志被覆盖如何通知**：

- 如果同一个 index 被新日志覆盖，apply 出来后 `op.ClientId/RequestId` 与等待者不一致；
- `PutAppend` 会检查 `raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId`，不一致就回 `ErrWrongLeader`；
- 如果该 index 永远不被提交，则靠超时返回。

**没有显式的失败通知机制**，完全靠"超时 + 身份校验"。

#### 4.5 客户端如何感知 NotLeader/领导变更？重定向还是重试？指数退避？

**感知**：

- RPC 成功但 `reply.err() == ErrWrongLeader`；
- RPC 失败（`ok == false`，连接错误/超时）。

**处理**：

```cpp
server = (server + 1) % m_servers.size();
continue;    // 不做退避
```

- 环形轮询，没有 leader hint；
- **没有指数退避**，也没有随机 jitter。

**问题**：选举窗口内客户端会高频轮询所有节点，放大无效 RPC；多个客户端会同步重试。改进：

- 服务端在 reply 里带 `leaderHint`；
- 客户端指数退避 + jitter；
- 区分"连接失败"和"明确不是 leader"。

#### 4.6 端到端线性一致的证明思路？

依赖四个不变量：

1. **Leader Completeness**：新 leader 拥有之前所有已提交的日志（由 `UpToDate` 选举限制 + 当前任期提交规则保证）；
2. **Log Matching**：同 index + 同 term 的条目之前的所有条目相同；
3. **提交规则**：只提交多数派确认的、当前任期的条目；
4. **Apply 顺序**：状态机按 log index 严格串行 apply。

推理：

- 每个操作（包括读）都对应一条日志，日志有一个全局唯一顺序；
- 一个写操作只有在提交并 apply 后才返回，所以返回时它已生效；
- 后续的读操作也进入日志，它的 index 必然在写之后，apply 时能看到写的结果；
- 去重表保证重试不会重复执行。

**边界**：这里证明的是"在 Raft 正确实现的前提下"的线性一致。本项目有若干实现偏差（尾部不截断、`CondInstallSnapshot` 恒 true、无 fsync），所以**严格意义上不能声称已验证线性一致**。

#### 4.7 同一 key 并发 Put 的最终顺序？客户端能看到自己的写吗？

**顺序**：由 **leader 把请求追加到日志的顺序**决定。`Raft::Start` 持 `m_mtx`，所有提案串行化；日志的 index 就是全局顺序。

**read-your-writes**：能。写请求只有在提交并 apply 后才返回 `OK`；之后同一客户端的读也走日志，且 index 更大，apply 时一定能看到自己的写。

**注意**：如果客户端在写超时（`ErrWrongLeader`）后重试，虽然第一次可能已经提交，但重试携带同一 `requestId` 会被去重，语义仍然是"恰好一次"。

---

## 五、持久化与崩溃恢复

#### 5.1 raftstate 与 snapshot 分别存什么、什么格式、什么频率？

| | raftstate | snapshot |
|---|---|---|
| 内容 | `currentTerm`、`votedFor`、`lastSnapshotIncludeIndex/Term`、全部日志 | 跳表全量 + 去重表 |
| 格式 | boost 文本归档（`BoostPersistRaftNode`），其中每条日志是 protobuf 的 `bytes` | boost 文本归档（`KvServer`），内层是跳表的 boost 归档 |
| 写入方 | `Raft::persist()` / `Snapshot()` / `InstallSnapshot()` | `Raft::Snapshot()` / `InstallSnapshot()` |
| 频率 | 每次 `Start`、每次 `AppendEntries1`（**包括空心跳**）、每次投票/选举、每次快照 | 快照触发时、安装快照时 |

**⚠️ 频率问题**：`AppendEntries1` 的 `DEFER persist()` 让每个心跳都重写整份日志；25 ms 一次 → 每秒 40 次。

#### 5.2 "先落盘、后应答"在代码哪一步？fsync 还是 fdatasync？吞吐影响测过吗？

**代码位置**：

- `RequestVote`/`AppendEntries1` 用 `DEFER { persist(); }`，在函数返回前执行；
- 5 参数包装函数随后调用 `done->Run()` 发应答；
- 所以是"persist → 解锁 → 发应答"。

**用的是 `flush()`，不是 `fsync()` 也不是 `fdatasync()`。** 只能保证数据离开用户态缓冲，进入内核 page cache。

**吞吐影响：❌ 未测。** 但可以推断：当前每条命令已经重写整份日志（O(日志长度)），再加 fsync 会再叠加一次磁盘延迟；正确做法是 WAL + group commit（批量 fsync），把摊薄后的成本控制在可接受范围。

#### 5.3 日志追加是每条 fsync 还是批量？group commit？崩溃在"已应答未落盘"会怎样？

**❌ 没有 fsync，也没有 group commit。**

**崩溃在"已应答未落盘"之间**：

- **进程崩溃（SIGKILL）**：page cache 由内核持有，数据仍在，重启后能读到 → 安全；
- **断电/内核 panic**：page cache 丢失 → 已应答的数据丢失 → **违反 Raft 的持久化要求**；
- 由于日志和快照是"先 truncate 再写"，断电还可能留下半截文件，恢复时解析失败。

#### 5.4 为什么用 Boost.Serialization？和 Protobuf 比优缺点？为什么不统一？

**为什么用 boost**：

- 直接支持 `std::vector`、`std::unordered_map` 等容器，不用为快照单独写 .proto；
- `text_archive` 调试友好（可读）；
- 与 `Op` 的序列化复用同一套机制。

**vs Protobuf**：

| | Boost.Serialization | Protobuf |
|---|---|---|
| 体积 | 文本，较大 | 紧凑二进制 |
| 版本兼容 | 弱，无字段标签 | 强，支持字段增删 |
| 跨语言 | C++ 为主 | 多语言 |
| 性能 | 一般（文本解析） | 快 |
| 依赖 | 头文件 + 库 | protoc + runtime |

**为什么不统一**：历史原因——RPC 消息用 protobuf（由 .proto 定义），而持久化对象（日志数组、跳表、去重表）直接用了 boost。结果是日志条目是"protobuf 字节嵌在 boost 数组里"，快照是"boost 套 boost"。

**更合理的设计**：全部用 protobuf（定义 `Snapshot`/`PersistedState` message），或自己写带版本号 + CRC 的二进制格式。

#### 5.5 双层恢复的严格顺序？raftstate 比 snapshot 新（或相反）如何判定权威？

**顺序**：

```text
1. Raft::init
   a. 置默认值（term=0, status=Follower, commitIndex=0, lastApplied=0, logs 清空）
   b. readPersist(persister->ReadRaftState())
      → 恢复 term / votedFor / lastSnapshotIncludeIndex/Term / logs
   c. 若 lastSnapshotIncludeIndex > 0 → lastApplied = lastSnapshotIncludeIndex
   d. 启动 tickers

2. KvServer 构造
   a. persister->ReadSnapshot()
   b. 非空 → ReadSnapShotToInstall → parseFromString → SkipList::load_file
```

**权威判定：❌ 没有判定。**

两个文件本应通过 `lastSnapshotIncludeIndex` 对齐，但代码没有校验：

- 如果 raftstate 的 `lastSnapshotIncludeIndex` 与 snapshot 文件对应的 index 不一致，会静默按 raftstate 的偏移去解释后续日志；
- 没有版本号、没有校验和、没有 manifest；
- 一个文件损坏时无法判断该回滚到哪个状态。

**改进**：manifest 记录 `(snapshotIndex, snapshotCRC, walStartOffset)`，启动时校验三者一致，不一致则拒绝启动或从最近的完整快照恢复。

#### 5.6 trunc 模式导致持久化失效——根因、怎么发现、如何验证？

**根因**：`Persister` 构造函数里用 `std::ios::out | std::ios::trunc` 打开两个持久化文件。每次进程启动都会把上一次的状态清空，紧接着 `KvServer` 就去 `ReadSnapshot()`，读到的必然是空。

**发现方式**：`benchmark/persist_probe.sh`——写入 → 重启 → 读回。修复前读到空串，修复后能读到完整内容。

**修复**：

- 构造时用 `app` 模式，文件不存在则创建、存在则保留；
- 读完后以 `app` 重新打开写流（否则默认 `ios::out` 会截断）；
- 读取用 `ss << ifs.rdbuf()` 整体读（`operator>>` 遇空白会截断）。

**验证**：重启后 `raftstatePersist<i>.txt` 非空，且能正确反序列化；压测脚本用 `CLEAN_STATE=0` 保留状态跑一轮恢复。

> ⚠️ 诚实说明：这几处修复在代码注释里有完整记录。面试时如果讲，应说明"这是代码里记录的历史修复，我通过探针复现并理解了它"，不要暗示是自己首次发现。

#### 5.7 数据目录损坏、半写入记录（torn write）怎么处理？有校验和吗？

**❌ 都没有。**

- 没有 CRC/校验和；
- 没有长度前缀（boost 文本归档靠解析器的格式判断）；
- 半写文件会让 `text_iarchive` 解析失败（可能抛异常，也可能读到错误值）；
- `Raft::init` 没有捕获解析异常。

**改进**：每条记录 `[magic][length][crc32][payload]`；恢复时校验，遇到不完整记录就截断到上一条完整记录（WAL 的标准做法）。

#### 5.8 只有一台节点的盘坏了，系统行为是什么？如何恢复？

**行为**：

- 该节点重启后读到空/损坏状态，作为 Follower 加入；
- leader 会发现它的 `nextIndex` 落后，通过 `AppendEntries` 或 `InstallSnapshot` 把它追上；
- 只要多数派还在，集群继续服务。

**⚠️ 但本项目有两个现实问题**：

1. 单节点重启会在 `Raft::init` 前被心跳打到 → 空 `m_persister` → 段错误（见 Q6/面试 Bug 清单第 1 条）；
2. 如果损坏的是 raftstate，`readPersist` 可能解析失败且没有异常处理。

**恢复流程（理论）**：清空该节点的数据目录 → 用相同 node id 启动 → 作为空节点从 leader 拉快照。因为没有动态成员变更，节点身份必须与配置一致。

---

## 六、RPC 框架与并发模型

#### 6.1 单端口多路径复用怎么做？消息头设计？如何路由到 Raft/KV 服务？

**同一个 `RpcProvider` 注册两个服务**，共用一个 muduo `TcpServer`/端口：

```cpp
provider.NotifyService(this);                    // "kvServerRpc"
provider.NotifyService(this->m_raftNode.get());  // "raftRpc"
provider.Run(m_me, port);
```

**消息格式**：

```text
[varint header_size][RpcHeader protobuf][args 字节流]
RpcHeader { service_name, method_name, args_size }
```

- 没有数字 ID，直接用**服务名字符串 + 方法名字符串**；
- `RpcProvider` 用 `m_serviceMap[service_name].m_methodMap[method_name]` 查到 `Service*` 和 `MethodDescriptor*`，再 `CallMethod`。

**问题**：

- 每个请求都要做字符串查找（可以改为 ID 查表）；
- 没有 request ID，响应无法与请求关联（对同步短连接没问题，对并发/流水线是致命的）。

#### 6.2 Protobuf Generic Service 的反射调用怎么做的？性能开销？零拷贝/对象池？

```cpp
google::protobuf::Message* request  = service->GetRequestPrototype(method).New();
request->ParseFromString(args_str);
google::protobuf::Message* response = service->GetResponsePrototype(method).New();
service->CallMethod(method, nullptr, request, response, done);
```

`CallMethod` 是生成类里重写的虚函数，内部 `switch(method->index())` → 调用业务重写的 5 参数方法。

**开销**：

- 每次请求 `new` 一个 request + 一个 response（堆分配）；
- `GetRequestPrototype().New()` 是虚调用 + 分配；
- 字符串查找 service/method；
- 没有对象池、没有 arena、没有零拷贝。

**改进**：缓存 `MethodDescriptor`、用 `Arena` 分配、复用消息对象（需要线程安全）。

**⚠️ 已知 bug**：客户端 `recv` 用 **1024 字节**固定缓冲，响应超过 1KB 会被截断，`ParseFromArray` 失败。这限制了单次响应/快照的大小。

#### 6.3 muduo 线程模型？几个 EventLoop？为什么定时器放协程、日志复制放独立线程？

**模型**：

- 每个 `RpcProvider` 一个 `EventLoop`（`m_eventLoop`），跑在 `KvServer` 构造函数创建的那个线程里；
- `setThreadNum(4)` → muduo 额外创建 4 个 IO 线程（one loop per thread）；
- 所以每个节点有 1 个主事件循环 + 4 个工作线程。

**为什么这样分工**：

| 组件 | 载体 | 原因 |
|---|---|---|
| 选举/心跳定时器 | `monsoon::IOManager` 协程 | 周期短（25 ms），用协程避免一个线程一个定时器；`usleep` 被 hook，不阻塞调度器 |
| `applierTicker` | 独立 `std::thread` | 需要持续轮询并推进 `applyChan`，与网络/定时器解耦 |
| 日志复制/拉票 | 每个 peer 一个 detached `std::thread` | RPC 是同步阻塞的，放在协程里会阻塞调度线程 |

**问题**：三套并发模型（协程 + 裸线程 + muduo 线程池）共享状态，锁和生命周期很容易出错；detached 线程意味着没有统一的生命周期管理。

#### 6.4 协程调度器自研还是库？协程与 muduo 如何协作？跨线程唤醒？

**❌ 不是 C++20 协程，是 ucontext 实现的用户态 fiber。**

`src/fiber/fiber.cpp`：

```cpp
makecontext(&ctx_, &Fiber::MainFunc, 0);
swapcontext(&(cur_thread_fiber->ctx_), &ctx_);
```

`monsoon::IOManager` 是项目引入的第三方/参考实现（注释与文档里称为 monsoon），不是本项目的核心创新。

**与 muduo 的协作**：**没有协作**。两者是并行的两套线程/事件循环，只通过共享状态和锁交互。跨线程唤醒依赖 fos 库自己的 epoll/timerfd 机制和条件变量，不与 muduo 的 EventLoop 集成。

**风险**：如果某个 fiber 阻塞在真正的阻塞 IO（而不是被 hook 的 sleep/socket），会卡住整个 IOManager 线程（`FIBER_THREAD_NUM=1`）。

#### 6.5 独立线程并发发送如何避免对同一 Follower 的 RPC 乱序？Raft 要求同节点 RPC 有序吗？

**❌ 本项目不保证顺序。**

- `doHeartBeat` 每 25 ms 为每个 peer 起一个 detached 线程；
- 上一轮的线程可能还没返回，下一轮又起一个；
- 同一个 `RaftRpcUtil`（同一个 `MprpcChannel`/socket）被多个线程并发使用。

**Raft 是否要求有序**：不严格要求。`AppendEntries` 是幂等的，`prevLogIndex/prevLogTerm` 和 term 检查能处理乱序/重复。乱序的代价是性能（旧请求可能被误拒，导致 `nextIndex` 回退）。

**但本项目的并发使用同一个 fd 是真正的 bug**：多个线程同时 `send`/`recv` 同一个 socket，响应可能被错误的线程读到，破坏消息边界。这是需要修的：每个 peer 一把发送锁，或每线程独立连接，或改为异步多路复用。

#### 6.6 慢节点如何影响整体？超时/并发上限/背压？怎么验证？

**现状：没有超时、没有并发上限、没有背压。**

- `MprpcChannel` 的 `connect`/`send`/`recv` 都是阻塞调用，没有设置 `SO_RCVTIMEO`/`SO_SNDTIMEO`；
- 一个慢节点会让每个 detached 发送线程长期阻塞；
- 每 25 ms 每 peer 起一个线程，慢节点会导致线程数持续增长 → 内存/调度压力；
- `doHeartBeat` 持 `m_mtx` 起线程，但线程本身在锁外阻塞，所以不会直接卡住心跳，但会累积。

**验证：❌ 没做过**。可以做的实验：用 `SIGSTOP` 暂停一个 follower，观察 leader 的线程数、内存和心跳延迟。

**改进**：RPC 超时、每个 peer 的发送队列 + 单发送线程（保序 + 背压）、快照/复制限流。

#### 6.7 心跳和日志复制会不会互相阻塞？同一连接还是不同连接？

**同一连接**：每个 peer 只有一个 `RaftRpcUtil` → 一个 `MprpcChannel` → 一个 `m_clientFd`。心跳和日志复制都走它。

**会互相阻塞**：

- 两者都是同步 RPC，共用同一个 fd；
- 多个线程并发使用同一个 fd，没有锁；
- 一个大的日志复制正在进行时，心跳可能被延迟（如果 fd 被占用/响应错配）。

**这实际上是一个正确性问题**，不只是性能问题。正确做法：每 peer 一个发送队列 + 单发送线程，或每线程独立连接。

#### 6.8 客户端连接管理：心跳、断线重连、连接数上限、半包/粘包？

**现状**：

- **短连接/懒连接**：`MprpcChannel` 构造时（`connectNow=true`，如 `RaftRpcUtil`）建立一次连接，之后复用 `m_clientFd`；失败时在 `send` 循环里重连；
- **没有心跳/保活**；
- **没有连接数上限**；
- **半包/粘包**：服务端 `OnMessage` 用 `buffer->retrieveAllAsString()`，假设一次回调就是一个完整消息。muduo 的 `Buffer` 本身会累积数据，但代码没有做"读够 header+args 再分发"的流式重组，因此**粘包/半包会解析失败**；
- 响应没有长度前缀，客户端固定 `recv` 1024 字节。

**改进**：消息帧 `[4 字节长度][payload]`，或使用 `LengthHeaderCodec`；连接池；超时与重连退避。

#### 6.9 优雅退出怎么做？正在提交的日志、等待的客户端请求如何收尾？

**❌ 没有优雅退出。**

- 父进程 `pause()`，脚本用 `pkill -9`；
- 没有信号处理（SIGTERM/SIGINT）；
- 退出时不会主动 flush（虽然每次写都 flush 了）；
- 正在等待的客户端请求：进程直接消失，客户端会收到连接错误并重试；
- detached 线程没有 join，析构时可能访问已释放对象。

**要做的**：捕获 SIGTERM → 停止接受新请求 → 等待在途请求 → 持久化 → 关闭 socket → 退出；用 `std::jthread`/显式 join 管理线程。

#### 6.10 线程数、协程数、核数怎么匹配？过度订阅？

**现状（`benchmark/README.md` §4）**：每个节点约 8 个常驻线程：

| 来源 | 数量 |
|---|---|
| muduo | 5（1 主 + 4 工作） |
| IOManager | 1（`FIBER_THREAD_NUM=1`） |
| applierTicker | 1 |
| 主线程（join） | 1 |

3 节点 × 8 = **24 个线程跑在 4 核上 → 6 倍过度订阅**。

外加每个心跳周期为每个 peer 起 detached 线程：3 节点时 leader 每秒约 80 个线程创建（`clone()` 系统调用）。

**结论**：这是 4 核上最痛的开销之一。改进：每个 peer 一个常驻发送线程 + 队列；调小 muduo 线程数；定时器合并。

---

## 七、C++20 与工程细节

#### 7.1 项目里实际用到哪些 C++20 特性？挑一个讲"不用会怎样"

**❓ 重要澄清：项目用 `-std=c++20` 编译，但代码里几乎没用 C++20 特性。**

实际搜索结果：没有 `concepts`、`requires`、`std::ranges`、`co_await`/`co_return`、`std::jthread`、`std::format`、`std::span`、`std::atomic_ref`、`<=>`。

实际使用的更早标准特性：

- `std::shared_ptr`/`std::make_shared`（C++11）
- `std::thread`、`std::mutex`、`std::condition_variable`（C++11）
- `std::chrono::steady_clock`（C++11）
- `std::atomic`（C++11）
- 结构化绑定（C++17，少量）

**"协程"不是 C++20 协程**，而是 ucontext 实现的用户态 fiber（见 6.4）。

**面试时的正确说法**：

> 项目以 C++20 标准编译，但核心没有依赖 C++20 的新特性；协程是 ucontext 的 fiber，不是 coroutine TS。真正用到的是 C++11 的线程/chrono/智能指针。如果重写，我会用 `std::jthread` 管理线程生命周期、用 `std::format` 统一日志、用 `std::span` 传字节，但这些都是改进项，不是现状。

#### 7.2 协程实现细节：promise_type/awaitable/awaiter？如何避免悬垂引用？

**❌ 不适用**：项目用的不是 C++20 协程，没有 `promise_type`/`awaitable`/`awaiter`。

fiber 的实现要点：

- `makecontext(&ctx_, &Fiber::MainFunc, 0)` 创建上下文；
- `swapcontext` 在调度器和 fiber 之间切换；
- 每个 fiber 有独立栈（默认大小在 fiber 库里配置）；
- 生命周期由 `IOManager` 管理，`scheduler()` 投递任务。

**悬垂风险**：`m_ioManager->scheduler([this] { this->leaderHearBeatTicker(); })` 捕获 `this`；如果 `Raft` 被销毁而协程还在跑，就是悬垂。本项目里 `Raft` 与进程同生命周期，所以没有暴露，但这是隐患。

#### 7.3 内存管理：日志、快照、RPC 消息的所有权？智能指针？循环引用？

**所有权**：

| 对象 | 方式 |
|---|---|
| `Raft::m_peers` | `vector<shared_ptr<RaftRpcUtil>>` |
| `Persister` | `shared_ptr`，Raft 和 KvServer 共享 |
| `applyChan` | `shared_ptr<LockQueue<ApplyMsg>>`，两边共享 |
| `KvServer::m_raftNode` | `shared_ptr<Raft>` |
| `waitApplyCh` | `unordered_map<int, LockQueue<Op>*>`，**裸指针 + 手动 delete** |
| RPC request/response | 框架 `new`，由 `done` 回调负责释放（muduo 风格） |
| `provider.NotifyService(this->m_raftNode.get())` | 裸指针，但对象由 shared_ptr 持有 |

**循环引用**：`KvServer` → `Raft`（shared_ptr），`Raft` 不持有 `KvServer`，所以没有环。

**风险**：

- `waitApplyCh` 的裸指针在 timeout/成功路径会 delete，但如果插入后进程异常退出，会泄漏（进程级，影响小）；
- `NotifyService` 传裸指针，`Raft` 的生命周期靠 `KvServer` 保证；
- 快照序列化时临时构造 `m_serializedKVData`，然后 clear，属于临时状态。

#### 7.4 并发原语选择：mutex / shared_mutex / atomic / condition_variable / 无锁队列，分别用在哪？为什么？

| 原语 | 使用位置 | 原因 |
|---|---|---|
| `std::mutex` | `Raft::m_mtx`、`KvServer::m_mtx`、`Persister::m_mtx`、`SkipList::_mtx` | 保护复合状态；临界区短 |
| `std::condition_variable` | `LockQueue::Pop/timeOutPop` | 阻塞等待队列非空，带超时 |
| `std::atomic` | ticker 里的 `atomicCount`、fiber 的 id 计数 | 单一计数器，无复合状态 |
| `shared_mutex` | ❌ 未使用 | 读写没有分离（读也走 apply 线程） |
| 无锁队列 | ❌ 未使用 | 复杂度高，当前瓶颈不在队列 |

**问题**：锁粒度较粗（`Raft::m_mtx` 覆盖了持久化 IO），`m_status` 还有无锁读。

#### 7.5 如何避免数据竞争？用过 TSan/ASan/UBSan 吗？报过什么？

**❌ 没有跑过 sanitizer。**

已知的数据竞争（静态阅读）：

- `leaderHearBeatTicker` / `electionTimeOutTicker` 在锁外读 `m_status`（`raft.cpp:325,489`）；
- `m_status`、`m_currentTerm` 在多处无锁读；
- `MprpcChannel::m_clientFd` 被多个线程并发 `send/recv`；
- `shared_ptr<int> votedNum/appendNums` 的 `++` 在锁内，暂时安全，但依赖调用者持锁。

**建议**：TSan 跑一遍 3 节点 + 压测，优先修 `m_status` 和 socket 共享。

#### 7.6 编译与构建：CMake 结构、依赖、编译时间、警告等级、CI？

- 每个 `src/*` 模块有自己的 `CMakeLists.txt`，通过 cache 变量 `src_<模块名>` 导出源文件；根 CMake 聚合成静态库 `skip_list_on_raft`；
- 源文件用 `aux_source_directory` 收集 → **新增/删除文件必须重新跑 cmake 配置**；
- 依赖：muduo、protobuf、boost、pthread、dl；C++20；
- **没有开启 `-Wall -Wextra -Werror`**（根 CMake 只设置了标准）；
- **没有构建/测试 CI**：`.github/workflows/` 下只有 `greetings.yml`（欢迎语）和 `update_readme_contributors.yml`（更新贡献者列表），不编译、不跑测试；
- 编译时间：中等（protobuf 生成文件较大，约 200KB+ 源码）。

#### 7.7 单元测试怎么组织？确定性测试？model checking？

**❌ 都没有。**

- `test/` 下是几个手动编译的小程序（`defer_run.cpp`、`format.cpp`、`run.cpp`），**没有接入 CMake**；
- 没有单元测试框架（gtest/catch2）；
- 没有确定性模拟器（可控时钟、可控随机、可控网络）；
- 没有 TLA+/TLC 模型检查。

**现有验证手段**：`benchmark/` 的压测、读回校验、`kill -9`/`SIGSTOP` 故障注入、持久化探针。

**如果重做**：把"确定性模拟器"作为第一优先级的基础设施（固定种子 + 虚拟时钟 + 可注入网络/磁盘）。

#### 7.8 日志/可观测性：结构化日志、trace id、指标？

**现状：只有 `DPrintf`。**

- printf 风格，无级别（INFO/WARN/ERROR）；
- 无 trace id / request id 关联；
- 无 metrics（提交延迟、apply 延迟、选举次数、复制失败次数）；
- 无 tracing；
- `Debug=true` 时每条 apply 还会打印整个跳表（O(n)）。

**改进**：结构化日志（JSON/日志库）、请求级 trace id、Prometheus 指标、慢请求采样。

---

## 八、压测与性能分析

#### 8.1 压测工具链的组成？

- `benchmark/bench.cpp`：多线程客户端，每线程独立 `Clerk`、私有 key 空间；统计吞吐、P50/P90/P99/max；写入后全量读回校验；看门狗线程区分"慢"和"挂死"。
- `benchmark/run_bench.sh`：一键起集群 → 压测 → 收集群；支持 `NODES/THREADS/OPS/MODE/CPUSET/FAULT`；启动前清 `test.conf`，默认清持久化状态（`CLEAN_STATE=0` 保留）。
- `persist_probe.sh`：验证"写入 → 重启 → 读回"。
- `dedup_test.sh`：两个客户端进程验证去重。
- 辅助脚本：`ops_scaling.sh`、`scaling_test.sh`。

**局限**：`Clerk::Put` 返回 `void` 且内部无限重试，`bench` 无法统计真实成功率；"完成 N 次"是完成速率，不是成功率。

#### 8.2 166 ops/s 怎么算？为什么低？瓶颈定位过程与证据？

**来源**：`benchmark/README.md` §5.5，3 节点、绑核 0–3、4 客户端线程、每线程 1200 次操作、默认配置（`Debug=true`、`maxraftstate=500`）。

| 线程 | 吞吐 | P50 | P99 |
|---:|---:|---:|---:|
| 1 | 41 ops/s | 20.63 ms | 31.44 ms |
| 2 | 83 ops/s | 20.57 ms | 31.67 ms |
| 4 | 166 ops/s | 21.09 ms | 34.61 ms |

**为什么低**（按贡献排序）：

1. **延迟被心跳周期锁死**：`Raft::Start` 追加日志后**不主动触发复制**，要等下一次 `leaderHearBeatTicker`（25 ms）。P50 ≈ 20.5 ms ≈ 心跳的一半 + RPC + apply。
2. **每条命令触发全量持久化**：`persist()` 重写整份日志；且 `AppendEntries1` 每次心跳也 persist。
3. **快照阈值 50 字节**：几乎每条命令都做一次全量快照（转储整个跳表）。
4. **Debug 日志**：`DprintfKVDB()` 每 apply 一条就遍历打印整个跳表，O(n) 输出，累计 O(n²)。
5. **线程过度订阅**：24 线程 / 4 核。

**证据**：

- P50 在 50/300/500 次操作下**完全不变**（20.5 ms）→ 不是数据量导致的；
- 吞吐随客户端线程数 1→2→4 线性增长（41/83/166）→ 命令被批进同一次 `AppendEntries`，固定心跳成本被摊薄；
- 代码层面 `Start` 里明确注释"新命令来了不会直接执行，而是等待 leader 的心跳触发"。

#### 8.3 P50 20.5 ms 与心跳/选举的关系？怎么定位到"被动等待心跳"？改成主动触发后数据？

**关系**：P50 ≈ 心跳间隔的一半（期望等待 12.5 ms）+ 一次 AE RPC + apply 间隔（最多 10 ms）+ 持久化。实测 20.5 ms 吻合。

**定位方法**：

1. 固定其他变量，只改操作数（50/300/500），P50 不变 → 排除数据规模；
2. 只改客户端线程数，P50 基本不变、吞吐线性上升 → 说明每个操作的延迟有一个固定来源；
3. 读 `Start` 实现，发现它不触发复制，只等 `leaderHearBeatTicker`。

**改成主动触发后的数据：❌ 没有测。**

代码里留有注释：

```cpp
// rf.timer.Reset(10) //接收到命令后马上给follower发送,改成这样不知为何会出现问题，待修正 todo
```

说明作者尝试过并回退了。面试时**不要编造数字**，可以说"没有测到有效数据；预期是把 P50 从心跳的一半降到接近一次 RPC 的 RTT，但需要处理并发发送和乱序问题"。

#### 8.4 吞吐随并发"线性增长"的结论：测到几线程？拐点？原因？

- 测到 **1/2/4** 线程：41/83/166 ops/s；
- **没有观察到拐点**（在 4 核上 4 线程已经完全饱和/过度订阅）；
- 原因：多个客户端的命令被批进同一次 `AppendEntries`，心跳的固定成本被摊薄；单次延迟不变。

**未测**：8/16 线程、更多节点、关闭 Debug、调整 maxraftstate 后的曲线。

#### 8.5 批处理具体批什么？提升倍数？

**❌ 未实现批处理。**

可以批的层次（按收益排序）：

1. **日志落盘**：把多条 WAL 记录合并一次 `fsync`（group commit）；
2. **AppendEntries**：现在已经把 `[nextIndex, lastLogIndex]` 一次性发送，属于"天然批"；
3. **Apply**：一次 apply 多条到状态机，减少锁/序列化次数；
4. **客户端请求**：leader 侧合并多个客户端请求到一次复制。

**提升倍数：没有实测数据**，不要编造。

#### 8.6 P99 是多少？长尾原因？C++ 没有 GC，长尾来自哪里？

**P99**（同 §5.5）：31.44 / 31.67 / 34.61 ms（1/2/4 线程）。**max 未在表中给出**。

**长尾来源**：

- 心跳抖动：25 ms 周期，如果刚好错过一轮，等待接近 25 ms；
- 快照：全量转储跳表 + 两次文件重写，可能几十毫秒；
- 线程调度/过度订阅：24 线程争 4 核；
- `persist()` 随日志增长变慢；
- 偶发选举（如果发生，尖峰可达数百毫秒）。

#### 8.7 绑核怎么做？NUMA/伪共享？perf 火焰图？热点函数？

- 绑核：`run_bench.sh` 用 `taskset`（默认 `0-3`），`CPUSET=""` 关闭；
- **没有做 NUMA 优化**（单机 4 核，通常单 NUMA 节点）；
- **没有做过 perf 火焰图**；
- **没有量化热点函数**；从代码推断热点是：`persistData`（boost 序列化）、跳表遍历打印、`DPrintf`/`std::cout`、锁竞争。

**建议**：先关 `Debug`，再做 `perf record -g`，重点看 `text_oarchive`、`memcpy`、`write`。

#### 8.8 和 etcd / LogCabin 做过对比吗？差距在哪？

**❌ 没有做过对比测试。**

**定性差距**：

- etcd：WAL + fsync + 批量提交 + ReadIndex + MVCC + 动态成员 + TLS + metrics，单组写吞吐通常在数千到数万 ops/s 量级；
- 本项目：无 fsync、无批量、读走日志、每条命令重写日志、快照阈值错误，实测百级 ops/s。

**不要给出没有测过的具体倍数。**

#### 8.9 如何证明线性一致性，而不只测吞吐？Jepsen 风格校验？

**❌ 没有做线性一致性检查。**

现有校验是"写后读回"：

- 写入 N 个 key，再全部读回，检查丢失/值错误；
- 这能发现"已确认写丢失"，但**无法验证并发历史是否线性一致**（例如读写交错时的顺序）。

**正确做法**：

- 记录每个操作的调用/返回时间与结果，构造历史；
- 用 Porcupine（Go）或 Jepsen 的 checker 验证是否存在合法的线性化顺序；
- 注入分区/崩溃，做长跑。

---

## 九、故障注入与缺陷定位

#### 9.1 故障注入框架？kill -9 / SIGSTOP / 网络分区怎么模拟？可控可复现？

**现状**（`run_bench.sh`）：

| 变量 | 模拟 |
|---|---|
| `FAULT=kill FAULT_NODE=i` | `kill -9` 节点 i |
| `FAULT=stop` / `cont` | `kill -STOP` / `kill -CONT`，模拟卡死/分区 |
| `FAULT_DELAY` | 压测开始后多少秒注入 |

**没有真正的网络分区**（没有 iptables/tc/用户态丢包）。`SIGSTOP` 只能模拟"进程冻结"，不能模拟"单向丢包/延迟/乱序"。

**可复现性**：依赖时间，不是确定性的；同一脚本多次跑可能落在不同阶段。

**改进**：用 `tc netem` 注入延迟/丢包、用 iptables 做分区、固定随机种子与操作序列、记录每次注入的精确阶段。

#### 9.2 写后读回校验的判定标准？如何区分系统错误与脚本时序问题？

**判定**：

- 写入的 key 读回应为最后写入的值；
- 统计"丢失"（读不到）和"值错误"；
- 仅对"已确认成功"的写做校验。

**区分系统错误与脚本问题**：

- 看门狗线程：如果完成计数长时间不动，说明是挂死而非慢；
- 校验失败时检查集群是否还在（`pgrep`）；
- 对比 `DPrintf` 日志中的 `CommandIndex/ClientId/RequestId`；
- 重跑同一场景，确认是否稳定复现。

**⚠️ 局限**：`Clerk::Put` 返回 `void`、内部无限重试，脚本无法知道某个操作是否"已确认"，只能假设"最终返回就是成功"。

#### 9.3 三个缺陷：现象 → 定位 → 根因 → 修复 → 回归

**缺陷 A：UUID 未播种 → 跨进程 clientId 冲突 → 静默丢写**

- 现象：两个客户端进程压测，第二个进程 300/300 次写入全部丢失，但全部返回 OK；
- 定位：`dedup_test.sh` 对比两个进程的 clientId，发现完全相同；
- 根因：`Clerk::Uuid()` 用 `rand()` 生成，但没有 `srand()`，每个进程的随机序列相同；
- 修复：用 `std::random_device` 播种，或直接用随机 UUID；
- 回归：`dedup_test.sh`，修复后两个进程各自写入、互不干扰。

**缺陷 B：Persister 构造用 trunc → 崩溃恢复从未生效**

- 现象：重启后状态为空；
- 定位：`persist_probe.sh` 写入 → 重启 → 读回，读到空；
- 根因：构造时用 `ios::out | ios::trunc`，每次启动清空文件；读快照时又用默认 `ios::out` 重开写流，再次截断；`ifs >> str` 遇空白截断；
- 修复：`app` 模式 + 整体读 + `flush`；
- 回归：重启后能读到完整内容；`CLEAN_STATE=0` 场景验证恢复。

**缺陷 C：SIGPIPE → 节点被杀时本端进程退出**

- 现象：`FAULT=kill` 后压测进程 `exit=141`（128+13）；
- 定位：信号 13 = SIGPIPE；对端关闭后 `send(fd, ..., 0)` 触发；
- 根因：没有处理 SIGPIPE，默认动作是终止进程；
- 修复：`send(..., MSG_NOSIGNAL)`，并处理 `EPIPE` 重连；
- 回归：`FAULT=kill` 后压测能正常完成。

> ⚠️ 其中 B 的修复在 `Persister.cpp` 注释里有记录，讲的时候注意归属。

#### 9.4 除了这三个，还有哪些难复现的问题？如何缩短复现路径？

- 单节点重启崩溃（初始化前发布服务）：需要单独重启一个节点才复现；缩短路径 = 在 `init` 前加 `sleep(30)` 再重启。
- 选举卡住/丢票（`votedNum` 数据竞争，README 中列为观察目标，未确认）：需要高并发 + 特定时序；缩短路径 = 注入延迟、固定种子、加日志。
- `CondInstallSnapshot` 的时序问题：需要快照消息与 apply 消息交错；缩短路径 = 在 push 前后加可控 sleep。
- RPC 1024 字节缓冲导致大响应失败：缩短路径 = 直接写一个 >1KB 的 value。

#### 9.5 偶发数据不一致 bug，没有 core dump，排查步骤？

1. **固定现场**：记录操作序列、种子、集群配置、各节点日志；
2. **缩小范围**：从 3 节点降到单节点/单客户端，排除并发因素；再从单节点加回；
3. **确定性重放**：固定随机种子，用可控时钟和网络模拟器重放；
4. **加不变量断言**：在关键位置检查（日志连续、commit ≤ lastLog、apply 顺序、去重表单调）；
5. **对比各节点状态**：同一 index 的 term/command、commitIndex、lastApplied；
6. **二分定位**：用日志中的 term/index 定位第一次分叉；
7. **工具**：ASan/TSan、`strace`/`perf`、线性一致性 checker；
8. **最后**：加回归用例，确保修复后不会复发。

---

## 十、扩展与对比

#### 10.1 和 etcd 的差异？为什么不直接用 raft 库？

**etcd**：Raft + MVCC + bbolt + WAL + watch/lease/transaction + 动态成员 + TLS + metrics，是生产级系统。

**本项目**：单组 Raft + 最简单的 KV + 自研 RPC + 跳表，没有 MVCC/watch/lease/事务/安全/成员变更。

**为什么不用 raft 库（raft-rs / etcd raft）**：学习目的。用库可以跳过共识实现，但仍要自己写网络、存储、状态机、快照、成员管理；而且看不到内部细节。生产环境应该用库或直接用 etcd。

#### 10.2 Multi-Raft / 分片怎么扩展？单组容量瓶颈？

**未实现。** 单组瓶颈：

- 所有写经过一个 leader，串行追加 + 复制；
- 日志和快照在单机上，磁盘/内存受限；
- 单组故障域 = 整个数据集。

**Multi-Raft**：按 key range 或 hash 分片，每片一个 Raft 组；需要 placement、路由、分裂/合并、跨片事务。TiKV 就是这个模型。

#### 10.3 成员变更怎么支持？3 → 5 要改什么？

**未实现。** 需要：

- 成员配置作为日志条目（joint consensus 或单节点变更）；
- `m_peers` 动态伸缩，`nextIndex/matchIndex` 同步调整；
- 快照包含成员配置；
- 启动时配置一致性校验；
- 领导权转移。

3→5：一次加一个节点，先在 joint 配置下取得新旧多数派，再切到新配置。

#### 10.4 节点数 5、RTT 变大，超时/快照阈值怎么调？自动调参？

- 选举超时 ≥ 10–20 × 心跳；心跳 ≥ 2–3 × P99 RTT；
- 节点数增加 → 广播时间上升 → 选举超时放大；
- 快照阈值应按"状态大小 + 落盘成本"定，不是固定的 500；
- 自动调参要有滞回和上限，避免振荡。本项目都没有。

#### 10.5 事务/范围原子写？TTL/二级索引？

- 事务：把多 key 操作编码成**一条日志条目**，状态机原子应用；跨分片需要 2PC。
- 范围写：利用跳表的有序性，遍历 level 0。
- TTL：不能用墙钟（各节点时钟不同），要用日志中的逻辑时间或 leader 提交时间，并在状态机中确定性过期。
- 二级索引：额外的跳表，在同一 apply 中更新，保证原子性。

#### 10.6 快照+日志改用 LSM（RocksDB）怎么改？

- 日志：用 WAL（或 RocksDB 的 WAL）追加；
- 状态机：RocksDB `WriteBatch` 原子应用；
- 快照：RocksDB checkpoint（硬链接/复制）；
- 需要确定性遍历和批量原子写；
- 与 etcd/TiKV 的路径一致。

#### 10.7 安全：鉴权/加密？伪造 clientId 会怎样？

**❌ 没有任何安全机制。**

伪造 clientId 的后果：

- 用别人的 clientId 发一个很大的 requestId → 对方后续请求全部被判为重复而**静默丢弃**；
- 可以读取/覆盖任意 key（没有鉴权）；
- 可以重放请求。

需要：mTLS、客户端证书/Token、服务端鉴权、按认证身份生成去重键、审计日志。

#### 10.8 最大技术亮点一句话？最大技术债？

**亮点**：完整实现 Raft 2A–2D，并把共识层与状态机通过 `ApplyMsg` 清晰解耦；同时用双坐标模型处理快照偏移，建立了可复现的故障注入与持久化验证手段。

**技术债**：持久化模型——全量重写、无 fsync/原子写，既限制耐久性，也锁死性能上限。

---

## 十一、高风险"陷阱题"

#### 11.1 数字自洽性：166 ops/s、P50 20.5 ms、4 线程

**推导**：

- 若 4 个线程完全并行且每 op 20.5 ms → 理论上限 `4 / 0.0205 ≈ 195 ops/s`；
- 实测 166 → 约 85% 的效率，差距来自 P99 长尾、调度、序列化与统计口径；
- 单线程：`1 / 0.0205 ≈ 48.8`，实测 41 → 约 84%，一致；
- P50 20.5 ms ≈ 心跳 25 ms 的期望等待（约一半）+ RPC/apply 开销，与"被动等心跳"的机制吻合。

**口径要统一**：166 是整轮压测的**平均完成速率**，P50 是该轮的操作延迟中位数；两者不是同一分母，不能直接相除后宣称精确一致。表中"操作数"一列的口径（总数还是每线程）在 README 里没有写明，引用前最好先复跑确认。

#### 11.2 "恰好一次"的表述

严格说，Raft 客户端语义是 **at-least-once + 幂等去重**，不是数学意义上的 exactly-once。正确表述：

> 客户端可能重试，服务端保证同一 `(clientId, requestId)` 只执行一次，因此对状态机表现为恰好一次。

局限：

- 去重表无界增长（本项目）；
- 若做淘汰，超出窗口的重试会被重复执行；
- clientId 可伪造。

#### 11.3 "线性一致性"验证

**只做了功能测试（写后读回），没有做一致性检查。** 要坦诚：

> 我验证的是"已确认的写入不丢失、值正确"，这能覆盖提交安全性的一部分，但不能证明并发历史可线性化。真正的验证需要 Jepsen/Porcupine 风格的 checker。

#### 11.4 回扫优化的正确性

回扫依赖"日志中 term 单调不减"这一 Raft 不变量：

- 从 prevLogIndex 向前，遇到第一个 term 不同的位置就停；
- 该位置之后直到 prevLogIndex 的所有条目 term 都相同，因此整段可跳过；
- 返回 `该位置+1`，即冲突 term 的第一个 index。

**边界**：`prevLogIndex < snapshotIdx` 的分支在前面就返回了，不会进入回扫；循环下界是 `snapshotIdx`，不会读到已裁剪日志。

#### 11.5 去重表与快照裁剪

**当前不裁剪**，所以不存在"很久之后的重试被重复执行"的问题——代价是无界增长。

如果将来裁剪，安全窗口 = 条目保留时间，必须 ≥ 客户端最大重试时间（含崩溃重启）。超出窗口后只能依赖客户端不再重试旧请求。

#### 11.6 协程 + 线程混合模型的风险

- 死锁：多把锁 + 协程切换，若在持锁时 yield，可能死锁；本项目 `m_mtx` 内不做 `usleep`/RPC，风险较低；
- 优先级反转：协程和 OS 线程共享 CPU，长任务会饿死短任务；
- **协程阻塞在同步 IO**：`IOManager` 只有 1 个工作线程（`FIBER_THREAD_NUM=1`），任何未被 hook 的阻塞调用都会卡住所有 fiber；
- 线程生命周期：detached 线程 + `this` 捕获，析构时可能悬垂。

#### 11.7 Boost.Serialization 的落盘原子性

**不提供**。它只负责序列化到流/字符串。要做到崩溃安全，需要：

`写临时文件 → fsync → rename → fsync 目录`，或者 WAL + 校验和。本项目没有做。

#### 11.8 单点写性能与 Multi-Raft

Raft 的写必须经过 leader 串行追加与复制，单组写吞吐有上限。若被问"为什么不上 Multi-Raft"：

> 当前是单组、单机、教学规模；Multi-Raft 需要分片、路由、成员管理和跨片事务，复杂度高一个数量级。单组的瓶颈在 leader 串行化和全量持久化，先把这两项解决（WAL + batching）收益更大。

---

## 十二、反问环节可以问的

1. 团队是自研共识还是用开源（etcd/raft-rs）？对可靠性验证（混沌测试、形式化）投入多少？
2. 存储引擎侧更看重吞吐还是尾延迟？现有瓶颈在哪？
3. 代码评审、压测与线上故障复盘的机制是怎样的？
4. 这个岗位会接触到一致性/存储的核心链路，还是更偏业务层？

---

## 准备建议（优先级）

1. **默写两张图**：一次 Put 的时序、一次 Leader 崩溃恢复的时序。
2. **三个缺陷做成 STAR 卡片**：各准备 90 秒版和 3 分钟版（现象 → 定位 → 根因 → 修复 → 回归）。
3. **性能数字统一成一张表**：并发数 / 吞吐 / P50 / P99 / 瓶颈结论，确保任何追问都能对上口径。
4. **每个"自研设计"准备"不用它会怎样"**：双坐标、回扫、双向快照、去重表——这是面试官判断是否真懂的核心手段。
5. **主动标注"未实现"**：PreVote、LeaseRead、成员变更、fsync、批处理、Jepsen、sanitizer、C++20 特性——说清楚"没做 + 该怎么做 + 代价"，比含糊其辞更可信。
