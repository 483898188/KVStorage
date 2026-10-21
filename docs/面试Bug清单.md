# 面试 Bug 清单：8 个可深挖的缺陷与讨论点

> 项目：自研 Raft 共识 + 分布式 KV（`KVstorageBaseRaft-cpp`）
> 用途：面试时挑 2~3 条讲深，而不是把 8 条平铺
> 相关文档：[leader选举](./leader选举.md)、[日志匹配](./日志匹配.md)、[持久化](./持久化.md)、[链路](./链路.md)
> 前序面试案例：[静默丢写](./面试案例-静默丢写.md)、[故障注入与持久化](./面试案例-故障注入与持久化.md)

---

## 0. 一句话版本

这个项目的**共识算法实现是扎实的**（选举、日志匹配、commit 规则都对），
缺陷集中在三处工程面：**对象生命周期**（先发布后初始化）、**持久化耐久性**（无 fsync / 非原子）、
**线程模型**（协程 + 裸线程 + muduo 线程池混用）。

下面 8 条里，**第 1、3、5、7 条最值得讲**，其余按需补充。

---

## 严重度总表

| # | 类型 | 一句话 | 严重度 | 面试价值 |
|---|---|---|---|---|
| 1 | 崩溃 | 节点重启后 Raft 在 `init` 前就被发布到网络上，心跳打进来 → 空 `shared_ptr` 解引用 | 🔴 致命 | ★★★★★ |
| 2 | 性能 | 空心跳也触发全量日志落盘，25 ms 一次，且在 Raft 锁内 | 🟠 高 | ★★★☆☆ |
| 3 | 逻辑 | 快照阈值算错，`proportion` 参数没用，几乎每条命令都做一次全量快照 | 🟠 高 | ★★★★★ |
| 4 | 正确性 | `CondInstallSnapshot` 恒返回 `true`，2D 的安全钩子没实现 | 🟠 高 | ★★★★☆ |
| 5 | 耐久性 | `Save()` 先 truncate 两个文件再写、无 `fsync`，崩溃窗口内全丢 | 🟠 高 | ★★★★★ |
| 6 | UB | 定时器线程无锁读 `m_status` | 🟡 中 | ★★★☆☆ |
| 7 | 架构 | 在 muduo RPC 工作线程里阻塞 500 ms，和心跳共用 4 个线程 | 🟡 中 | ★★★★☆ |
| 8 | 小 bug | `DPrintf` 用 `%s` 打 `std::string*` / `%v` 不是 printf 格式符 | 🟢 低 | ★★☆☆☆ |

---

## 0.5 先更正一个错误结论

曾有一个流传较广的判断："`sendRequestVote` 里 `myAssert(false, "同一个term当两次领导")` 在 4 节点集群可达"。
**这个结论是错的**，它不可达。

```cpp
// src/raftCore/raft.cpp
if (*votedNum >= m_peers.size() / 2 + 1) {   // m_peers.size() 是节点总数，含自己那槽
    *votedNum = 0;                            // 当选后清零
    if (m_status == Leader) myAssert(false, "同一个term当两次领导");
```

设节点数 `n`。当选需 `n/2 + 1` 票，其中自己 1 票，故需外部赞成票 `n/2` 张。
当选后仍在飞的拉票线程数为 `(n-1) - n/2`。要再次触发断言，需这些线程**全部**返回赞成：

```
(n-1) - n/2  >=  n/2 + 1
=>  n - 1     >=  n + 1        // 恒假
```

因此它是**防逻辑错误的死代码**，不能当 bug 讲。特此更正。

---

## 🔴 1. 重启的节点会在 6 秒窗口内段错误

### 现象

单节点重启后，进程在启动后不久直接 `SIGSEGV`，日志停留在初始化阶段，没有任何业务输出。

### 代码事实

```cpp
// src/raftCore/kvServer.cpp
m_raftNode = std::make_shared<Raft>();                    // :383
std::thread t([this, port]() {
    RpcProvider provider;
    provider.NotifyService(this);
    provider.NotifyService(this->m_raftNode.get());       // :392  ★ 已经对外发布
    provider.Run(m_me, port);
});
t.detach();
sleep(6);                                                 // :399
...
m_raftNode->init(servers, m_me, persister, applyChan);    // :425  ★ 6 秒后才初始化
```

### 触发条件

**单节点重启**（其余节点仍在运行）。它的 `RpcProvider` 在毫秒级就监听端口了，
而 leader 每 25 ms 给它发一次心跳。

### 崩溃链路

```
leader 的 AppendEntries 到达
 → Raft::AppendEntries(5参数) → AppendEntries1()
   → if (args->term() < m_currentTerm) ...      // m_currentTerm=0，args.term>=1，不进入
   → DEFER { persist(); }                       // raft.cpp:24
      → m_persister->SaveRaftState(data)        // m_persister 默认构造 = 空 shared_ptr
      → 解引用空指针 → SIGSEGV
```

`Raft` 构造完成后、`init()` 之前，`m_persister`（`raft.h:38`，`std::shared_ptr<Persister>`）
是默认的空指针。而 `AppendEntries1` 的第一件事就是挂一个 `DEFER persist()`——
**不管日志匹配成不成功，退出时都会 persist**。

`RequestVote` 的 `DEFER persist()` 同理，所以拉票请求也能触发同样的崩溃。

### 为什么平时没暴露

`raftCoreRun` 一次性 fork 三个节点，靠 `sleep(6)` + `sleep(n-me)` 的时序把所有节点错开，
窗口刚好被盖住。只要单独重启一个节点，就是必现的崩溃。

### 根因

**对象在初始化完成之前就被发布到了网络上**（unsafe publication），
而唯一的保护措施是一个 `sleep`——它不是同步原语。

### 修复

把 `init()` 挪到 `NotifyService` 之前（`init` 只依赖 peers 列表，不依赖 RPC 是否连通）；
或者让 `Persister` 在 `Raft` 构造时就注入，而不是 `init` 时注入。

### 验证方式

起 3 节点 → `kill -9` 其中一个 → 单独重新拉起它 → 观察是否秒崩。
更快的办法：在 `Raft::init` 里加 `sleep(30)`，然后重启任一节点，几乎立刻能复现。

### 面试怎么讲

这是"生命周期与并发可见性"问题，不是 Raft 算法问题。
讲清"发布早于初始化"这个模式，以及为什么 sleep 不能替代同步——这两句话就能体现工程判断力。

---

## 🟠 2. 空心跳触发全量日志落盘

### 代码事实

`AppendEntries1` 里 `DEFER { persist(); }`（`src/raftCore/raft.cpp:24`）位于任期检查之后、
日志匹配之前。**每一次 AE 都会走到它**，包括 `entries_size() == 0` 的空心跳。

而 `persist()` 会：

```
persistData() → boost 序列化整个 m_logs（每条 LogEntry 再 protobuf 序列化）
m_persister->SaveRaftState(data) → clearRaftState() 截断文件 → 重写全部 → flush
```

### 量级

`HeartBeatTimeout = 25 ms`（`src/common/include/config.h:11`）→ 每个 follower
**每秒 40 次**把整份日志序列化并重写磁盘。日志 10 MB 时就是 400 MB/s 的磁盘写。
而且全程持有 `m_mtx`。

### 修复

只在 `currentTerm`、`votedFor`、`m_logs` 三者**真的变了**时落盘
（dirty 标志，或比较 `(term, votedFor, lastLogIndex, snapshotIndex)`）。

### 面试怎么讲

这是典型的"把论文的'必须持久化'理解成了'每收到消息就持久化'"。
正确性没错，但完全丢掉了批量和增量。Raft 的持久化要求是**在对外承诺前**落盘，
不是每条消息后。

> 注意：这是性能设计问题，不是正确性 bug。表述时要说"代价"，不要说"错误"。

---

## 🟠 3. 快照阈值算错，几乎每条命令都做一次全量快照

### 代码事实

```cpp
// src/raftCore/kvServer.cpp:192
IfNeedToSendSnapShotCommand(message.CommandIndex, 9);   // proportion = 9

// src/raftCore/kvServer.cpp:342
void KvServer::IfNeedToSendSnapShotCommand(int raftIndex, int proportion) {
  if (m_raftNode->GetRaftStateSize() > m_maxRaftState / 10.0) {   // ★ proportion 没用到
```

`maxraftstate` 传的是 `500`（`example/raftCoreExample/raftKvDB.cpp:58`），
于是阈值是 **50 字节**。而一份 raftstate（boost 归档 + term/votedFor + 至少一条 `LogEntry`）
轻松就超过 50 字节。

### 后果

第一条命令之后，**每 apply 一条命令就做一次快照**。而一次快照 =
`SkipList::dump_file()` 全量遍历 + boost 序列化整个 `KvServer` +
截断并重写 `raftstatePersist<i>.txt` 和 `snapshotPersist<i>.txt` 两个文件。

### 根因

两处叠加：

1. 调用方传了 `9`，函数体却硬编码 `/10`，`proportion` 参数是死的；
2. 分母 10 又把阈值降到了 `maxraftstate` 的 1/10。

按意图应该是 `m_maxRaftState * proportion / 10`（= 450）。

### 面试怎么讲

这是"参数契约失效"的典型案例：签名承诺了可配置的比例，实现却忽略了它，
于是调用方传什么都一样；同时分母 10 又把阈值降到了 1/10——一层 bug 掩盖另一层。
修一行就够，但需要先意识到签名和实现已经对不上。

**逻辑 bug、修复一行、论证清晰**，是这份清单里性价比最高的一条。

---

## 🟠 4. `CondInstallSnapshot` 永远返回 `true`

### 代码事实

```cpp
// src/raftCore/raft.cpp:183
bool Raft::CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot) {
  return true;
  //// Your code here (2D).
  // if lastIncludedIndex <= rf.commitIndex { return false }
  // ...
}
```

这不是"简化版"，是 **2D 的核心安全判断没实现**。它的职责是原子地回答
"这个快照现在还需要装吗"，参考实现的判据是 `lastIncludedIndex <= commitIndex → false`。

### 影响

`KvServer::GetSnapShotFromRaft` 拿到 `ApplyMsg` 后先问 `CondInstallSnapshot`，
返回 true 才 `ReadSnapShotToInstall` 覆盖整个状态机。
因为恒为 true，**任何到达的快照都会被无条件安装**。

### 更隐蔽的一层：消息顺序无保证

`InstallSnapshot` 推送快照消息用的是 `std::thread(...).detach()`（`raft.cpp:475`），
并且推送前已释放锁；而 `applierTicker` 也在往**同一个 `applyChan`** 推常规 `ApplyMsg`。
两者顺序没有任何保证。

若"快照消息"排在"快照点之后的某条命令"之后：

1. `KvServer` 先执行该命令（作用于旧状态机）；
2. 随后快照消息到达，`ReadSnapShotToInstall` 用旧状态覆盖；
3. `m_lastSnapShotRaftLogIndex` 被设为快照点，那条已出队的命令**不会再被重放**。

结果：**已提交的操作被静默丢失**。

> 触发需要特定交错（follower 的 `commitIndex` 大于快照点，且两个推送线程调度顺序颠倒），
> 但修复方向是清楚的：让"检查 + 安装"原子化，并保证快照消息与普通 apply 消息的全局顺序。

### 面试怎么讲

不要只说"这里有个 TODO"。讲清 *为什么* 需要这个钩子（快照消息与普通 apply 消息的竞态），
以及它返回 true 时丢的是哪一步——这能把一个 TODO 讲成对 2D 设计意图的理解。

---

## 🟠 5. `Persister::Save()` 非原子 + 无 `fsync`

### 代码事实

```cpp
// src/raftCore/Persister.cpp
void Persister::Save(const std::string raftstate, const std::string snapshot) {
  clearRaftStateAndSnapshot();     // 两个文件都 truncate 成空
  m_raftStateOutStream << raftstate;
  m_snapshotOutStream << snapshot;
  flush(); flush();                // 没有 fsync
}
```

### 两个问题

1. **崩溃窗口**：truncate 之后、写完之前崩溃 → **两份状态同时为空**。
   而且 raftstate 里的 `lastSnapshotIncludeIndex` 必须和 snapshot 的内容对应，
   一个有一个没有就是不一致状态，`readPersist` 之后日志偏移会错位。
2. **`flush()` ≠ 持久化**：只把数据交给内核 page cache，机器掉电 / 内核 panic 仍会丢。
   代码注释自己也承认了这点（"要抗断电还需要 fsync，本项目从简"）。

### 标准修法

写临时文件 → `fsync` → `rename()`（同文件系统内原子）→ `fsync` 目录。
两条写路径（`Save` 和 `SaveRaftState`）都应走这个流程。

### 面试怎么讲

这是"持久化（persistence）"与"耐久性（durability）"的区别。
能说出 `flush` / `fsync` / 原子 rename 三层差别，比背 Raft 伪码有含金量。

### 附带的小问题

`Save()` 调用 `clearRaftState()` 会把 `m_raftStateSize` 置 0，但 `Save()` 里**没有重新累加**。
于是刚装完快照的瞬间 `GetRaftStateSize() == 0`，快照阈值判断暂时失效。
影响很小（下一次 `persist()` 会修正），但属于"两处维护同一份状态却没同步"的疏漏。

---

## 🟡 6. `m_status` 在定时器线程里无锁读

### 代码事实

```cpp
// src/raftCore/raft.cpp —— electionTimeOutTicker（协程）
while (m_status == Leader) usleep(HeartBeatTimeout);        // :325

// src/raftCore/raft.cpp —— leaderHearBeatTicker（协程）
while (m_status != Leader) usleep(1000 * HeartBeatTimeout); // :489
```

而 `m_status` 在 `doElection`、`AppendEntries1`、`RequestVote` 等处都是**持锁写**的。
一读一写、无同步 → C++ 内存模型下的 data race，UB。

`m_currentTerm` 等也有同样的读法（可对照 `leaderSendSnapShot` 里那次持锁读）。

### 修复

加锁读，或改成 `std::atomic<Status>`。
（注意：原子只解决可见性，`if (m_status == Leader) { ... }` 这种复合判断仍需锁。）

### 面试价值

能带出"为什么 Raft 实现建议把状态机单线程化"——如果所有状态变更都在一个线程里，
这类竞态就不存在。这个项目的混杂结构（协程 + 裸线程 + muduo 线程池）正好是反面教材。

---

## 🟡 7. 在 RPC 工作线程里阻塞 500 ms

### 代码事实

```cpp
// src/raftCore/kvServer.cpp —— KvServer::PutAppend(2参数)
chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp);   // 最多阻塞 500 ms
```

这个函数是在 `RpcProvider` 的 muduo 线程池里执行的（`src/rpc/rpcprovider.cpp:91`，
`setThreadNum(4)`）。而**同一个 `RpcProvider` 同时挂着 `kvServerRpc` 和 `raftRpc` 两个服务**：

```cpp
provider.NotifyService(this);                    // "kvServerRpc" → 客户端流量
provider.NotifyService(this->m_raftNode.get());  // "raftRpc"    → 对端节点流量
```

客户端请求阻塞掉的线程，正好是处理 `AppendEntries` 心跳的线程。
**3 个并发写请求就能把心跳线程池占满**，进而诱发无谓的选举。

### 修复方向

- 请求进来只做"提交 + 登记"，立刻返回，结果用异步回调 / 独立线程等待；
- 或者把 raft 服务拆到独立的 provider / 线程池。

### 面试价值

高。这是"阻塞式 RPC 处理 + 共用线程池"的经典问题，和具体算法无关，通用性很强。

---

## 🟢 8. 两个小 bug（细节敏感度）

### 8a. `DPrintf` 用 `%s` 打 `std::string*`

`DPrintf` 是 printf 风格（`src/common/util.cpp:58`，内部 `std::vprintf`）：

```cpp
DPrintf("...ClientId %s, ... Operation %s, Key :%s...",
        &op.ClientId, ..., &op.Operation, &op.Key);
//       ↑ 传的是 std::string* 而不是 c_str()
```

`%s` 期望 `const char*`，实际收到 `std::string*`，`vprintf` 会把对象起始地址当成字符数组来读——
在 libstdc++ 上读到的通常是指针的低位字节，输出乱码甚至越界读（UB）。
出现位置：`src/raftCore/kvServer.cpp:173,253,264,329,338`；
`src/raftCore/raft.cpp:976` 的 `%s` + `&command`（`Op*`）同理。

### 8b. `%v` 不是 printf 的格式符

`src/raftCore/kvServer.cpp:328,337` 里有 `Opreation {%v}, Key :{%v}`——
`%v` 是 Go 的写法，C 里未定义。

### 面试价值

适合当补充，证明"逐行读代码"的耐心。不要当主线讲。

---

## 面试取舍建议

优先讲 **1、3、5、7**：

| 选择 | 理由 |
|---|---|
| **1**（重启段错误） | 自己推出来的（代码注释里没有），最能体现"读代码发现问题" |
| **3**（快照阈值） | 逻辑 bug，修复一行、论证清楚，性价比最高 |
| **5**（非原子落盘） | 能展开到 durability 的通用知识 |
| **7**（阻塞线程） | 架构层面的判断，不依赖这个项目的细节 |

**2 和 4 要小心表述**：

- 2 是"性能选择"而非正确性错误，说成 bug 可能被追问"那它算错吗"；
- 4 是明确的未完成实现，直接说"这是个 TODO 且未实现"即可，别包装成自己的发现。

---

## 附录：哪些**不建议**讲

`src/raftCore/Persister.cpp` 里那四段注释记录了**别人已经修过**的几个 bug：

1. 构造函数用 `std::ios::trunc` 打开文件 → 每次启动抹掉上次落盘的状态；
2. `ReadSnapshot` 重开流用默认 mode（`ios::out`）→ 每次读快照就把文件清零；
3. 读文件用 `ifs >> str` → operator>> 遇空白截断，只能读到第一个词；
4. 写完不 `flush()` → 被 SIGKILL 时丢失。

这四个确实很精彩，但**代码注释里白纸黑字写着**。如果讲，一定要说明
"这是代码注释里记录的历史修复，我是在读代码时理解的"，**不要暗示是自己发现的**——
面试官看代码就能看到注释。

---

## 一句话收尾（可直接用）

> 这个项目的正确性实现是扎实的——选举、日志匹配、commit 规则都忠实于论文。
> 短板集中在三处工程面：**生命周期管理**（对象先发布后初始化）、
> **持久化耐久性**（无 fsync / 非原子 rename）、
> **线程模型**（协程 + 裸线程 + muduo 池混用，导致无锁读和不必要的阻塞）。
> 这三类问题在原版 MIT 6.824 代码里也普遍存在，不是这个作者特有的。
