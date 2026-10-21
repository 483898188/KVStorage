# 怎么讲 Raft 这一块

> 项目：自研 Raft 共识 + 分布式 KV（`KVstorageBaseRaft-cpp`）
> 用途：面试口述稿 + 追问预案
> 配套材料：[leader选举](./leader选举.md)、[日志匹配](./日志匹配.md)、[持久化](./持久化.md)、[链路](./链路.md)、[面试 Bug 清单](./面试Bug清单.md)

---

## 0. 核心原则

面试讲 Raft，**不要按代码模块讲**（"我写了 raft.cpp、kvServer.cpp……"），而是按**一条请求的生命周期**讲。

面试官想确认的其实是三件事：

1. 你知不知道 Raft 的**三个安全性不变量**；
2. 你能不能说清**自己的实现和论文伪码的差异**；
3. 你有没有**验证过**——或者至少知道哪里没验证。

---

## 1. 按时间长度准备三档

| 时长 | 内容 |
|---|---|
| **30 秒** | 从零实现 Raft 2A–2D：选举、日志复制、持久化、快照；上层自研 KV 状态机与客户端，底层自研 RPC 框架。重点讲"一条写请求怎么走完全程"。 |
| **2 分钟** | 上面 + 三个不变量 + 两个有代表性的难点（快照偏移 / 日志冲突回退）+ 一个主动暴露的问题。 |
| **5 分钟** | 上面 + 一条完整的写链路 + 快照双向 + 线性一致的落点 + 工程短板与改进方向。 |

---

## 2. 两分钟讲稿（可直接背）

> 这个项目是从零实现的 Raft，覆盖论文的 2A–2D：领导者选举、日志复制、持久化和快照。
> 上层是一个 KV 状态机，底层是自己写的基于 muduo + protobuf 的 RPC 框架。
>
> **架构上分四层**：传输层（mprpc，只认 `google::protobuf::Message`）、
> 服务骨架层（protoc 生成的服务基类）、共识层（`Raft` 继承 `raftRpc`，同时持有全部状态）、
> 状态机层（`KvServer`）。`Raft` 和 `KvServer` 之间只有三根线：一个 `Start()` 调用、
> 一个共享的 `LockQueue<ApplyMsg>`、一个共享的 `Persister`。
>
> **一条写请求**：客户端带 `(clientId, requestId)` 发给 leader
> → `KvServer` 把命令封装成 `Op` 调 `Raft::Start`
> → `Start` 先落盘再返回日志下标 `raftIndex`，同时 `KvServer` 按这个下标在 `waitApplyCh`
> 挂号并阻塞
> → leader 心跳时把日志复制到多数派
> → 提交后 `applierTicker` 把 `ApplyMsg` 推到 `applyChan`
> → 状态机按 `CommandIndex` 去重、执行、唤醒挂起的 RPC。
>
> **线性一致的落点**在状态机的去重：客户端重试时 `requestId` 不变，
> 服务端用 `m_lastRequestId` 判重，所以"响应丢了再重试"不会重复执行。
> 去重表会随快照持久化，重启也不丢。
>
> 我重点处理了两个地方：一是**快照带来的两套下标**（逻辑 `logIndex` 和 vector 下标要换算），
> 二是**日志冲突时的精确回退**——follower 回扫找到 term 变化点，而不是简单地 `nextIndex--`。
>
> 另外我做过一轮故障注入和持久化探针，发现并定位了几个问题，
> 比如恢复路径实际上从来没生效过、以及节点重启时对象在初始化前就被发布到网络上。
> 这些我都写了分析文档。

**最后一段是加分项**：它证明你不是"跑通了就算完"。

---

## 3. 必须能讲清的六个设计点

按被问到的概率排序。

### 3.1 三个安全性不变量怎么保证的

| 不变量 | 靠什么 |
|---|---|
| **选举安全**（每任期最多一个 leader） | `votedFor` + `currentTerm` 持久化；`RequestVote` 先落盘再回包 |
| **日志匹配**（同 index+term 则前缀相同） | `prevLogIndex`/`prevLogTerm` 检查，`matchLog` |
| **Leader 完备性**（新 leader 含所有已提交日志） | `UpToDate` 选举限制 + **只提交当前 term 的日志** |

第三点是区分度最高的。能说出"为什么 leader 不能直接提交之前 term 的条目"
（论文 §5.4.2）就已经超过多数候选人了。

### 3.2 选举怎么避免活锁

随机超时 **300–500 ms**，心跳 **25 ms**，差一个数量级；
心跳只推移 `m_lastResetElectionTime` 这个**起点**，不修改超时时长。

### 3.3 日志冲突怎么回退

不是无脑 `-1`：follower 回扫找到 term 变化点，返回精确的 `updatenextindex`；
另外两个越界分支分别返回 `lastLogIndex + 1` 和 `snapshotIdx + 1`。

### 3.4 快照下标的处理（本项目的特色）

`getSlicesIndexFromLogIndex` 是唯一换算点：

```cpp
return logIndex - m_lastSnapshotIncludeIndex - 1;
```

快照点被当作一条**虚拟日志**参与匹配——`getLogTermFromLogIndex` 对
`logIndex == snapshotIdx` 特判返回快照 term。能讲清这个抽象，说明真读懂了代码。

### 3.5 快照的双向流程

- **本地产生**：`IfNeedToSendSnapShotCommand` → 序列化整个 `KvServer` → `Raft::Snapshot` 截断日志。
- **远端安装**：leader 发现 `nextIndex <= snapshotIdx` → `InstallSnapshot` →
  follower 推一个 `SnapshotValid` 的 `ApplyMsg`，复用同一条 apply 管道。

### 3.6 GET 为什么也走共识

因为本实现用"把读也写进日志"来保证线性一致——简单、绝对正确，
但代价是每个读都要落盘并复制。

> **主动说出替代方案**：ReadIndex / leader lease read，
> 前者只需要一次多数派心跳确认。这题几乎必问，答"不知道还能优化"会掉分。

---

## 4. 追问应答表

| 追问 | 回答要点 |
|---|---|
| **怎么保证不重复执行？** | `(clientId, requestId)`；客户端重试 ID 不变；服务端 `requestId <= m_lastRequestId` 判重；去重表随快照持久化 |
| **网络分区会怎样？** | 少数派选不出 leader、无法提交；多数派继续工作。**主动补一句**：没有 PreVote，分区节点高任期回归会打断正常 leader，这是已知短板 |
| **leader 挂了，客户端怎么办？** | RPC 失败或 `ErrWrongLeader` → 环形切到下一台，同一 `requestId` 重试 → 打到新 leader |
| **怎么测试的？** | **坦诚**：没有确定性测试框架，靠手工起 3 节点 + 压测脚本 + `kill -9` 故障注入 + 持久化探针。**然后说你知道正确做法**：确定性随机种子 + 可注入延迟/分区/崩溃的模拟器（6.824 tester 那套） |
| **和 etcd 比差在哪？** | 差在工程化：batch/pipeline、WAL + fsync + 原子 rename、ReadIndex、PreVote、动态成员变更、指标与混沌测试。算法理解够格，工程差几年 |

**关于测试的诚恳反而加分**：说"我验证了哪些、没验证哪些"，比声称"全对"可信得多。

---

## 5. 主动暴露问题的正确姿势

选 **2 条**，按「现象 → 根因 → 影响 → 修复」讲，不要只说"有个 bug"。

### 5.1 节点重启段错误

> 重启的节点在 `init()` 之前就把 `Raft` 对象注册到了 RPC 服务上，
> 心跳打进来触发 `persist()`，而 `m_persister` 还是空 `shared_ptr` → 段错误。
> 根因是"先发布后初始化"，用 `sleep` 当同步原语。
> 修复是把 `init` 挪到 `NotifyService` 之前。

### 5.2 快照阈值参数失效

> 签名收了 `proportion` 却硬编码 `/10`，调用方传 9 完全没生效，
> 阈值从 450 变成 50 字节，结果几乎每条命令都做一次全量快照。
> 根因是"参数契约失效 + 两层错误互相掩盖"。

这两条的共同优点：**修复小而明确，论证清晰，且不是代码注释里写的**。

> ⚠️ 不要讲 `Persister.cpp` 注释里记录的那四个修复——那是别人的工作，代码里白纸黑字。

---

## 6. 不要说这三句话

1. ❌ **「我实现了完整的 Raft」**
   完整 Raft 还包括 PreVote、成员变更、领导权转移、快照安装的原子性。
   改成「实现了 2A–2D 的核心部分」。

2. ❌ **「没有 bug」**
   这个项目里至少有一个必现的崩溃和若干未实现的钩子，说没有会被当场拆穿。

3. ❌ **「用了协程所以性能好」**
   实际是协程 + 裸线程 + muduo 线程池三套混用，`m_status` 还有无锁读。
   性能恰恰是短板。

---

## 7. 一句话收尾

> Raft 的算法本身不难，难的是**失败模式**。
> 这个项目让我理解到：正确性靠论文，可用性靠对"崩溃、分区、丢消息、重启"每一种情况的显式处理——
> 而这些恰恰是代码里最容易漏、测试里最容易没覆盖的地方。

这句话既总结了项目，也把话题引向了你准备好的"工程短板与改进方向"，是收尾的好位置。

---

## 附录：一页速查

### 关键数字

| 参数 | 值 | 来源 |
|---|---|---|
| 心跳间隔 | 25 ms | `config.h:11` |
| 选举超时（随机） | 300–500 ms | `config.h:14-15` |
| 客户端等待提交超时 | 500 ms | `config.h:17` |
| apply 间隔 | 10 ms | `config.h:12` |
| 快照阈值 | `m_maxRaftState / 10.0` = 50 字节 | `kvServer.cpp:343` |
| 默认节点数 / maxraftstate | 3 / 500 | `raftKvDB.cpp:58` |

### 关键数据结构

| 名称 | 位置 | 作用 |
|---|---|---|
| `m_logs` | `raft.h:42` | `vector<LogEntry>`，Raft 日志本身 |
| `m_nextIndex` / `m_matchIndex` | `raft.h:48-50` | 每个 peer 的复制进度，下标 = server id |
| `applyChan` | `raft.h:55` | `LockQueue<ApplyMsg>`，Raft → KvServer |
| `waitApplyCh` | `kvServer.h:38` | `raftIndex → LockQueue<Op>`，唤醒挂起的 RPC |
| `m_lastRequestId` | `kvServer.h:41` | `clientId → requestId`，幂等去重 |
| `BoostPersistRaftNode` | `raft.h:144` | 持久化的 5 个字段 |

### 三个不变量对应的关键函数

| 不变量 | 函数 |
|---|---|
| 选举安全 | `RequestVote` + `persist()` |
| 日志匹配 | `matchLog` / `AppendEntries1` |
| Leader 完备性 | `UpToDate` + `sendAppendEntries` 的 term 判断 |

### 推荐阅读顺序

1. [链路](./链路.md) —— 先建立全局流程
2. [leader选举](./leader选举.md) —— 再看状态机怎么动
3. [日志匹配](./日志匹配.md) —— 然后是正确性的核心
4. [持久化](./持久化.md) —— 最后是崩溃恢复
5. [面试 Bug 清单](./面试Bug清单.md) —— 准备"主动暴露"环节
