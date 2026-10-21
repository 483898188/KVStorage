# raftCore 模块说明

> Raft 共识算法的完整实现，以及把共识层与状态机对接起来的服务层。
> 本文面向"逐行读实现"的读者，是 [raftClerk 说明](../raftClerk/raftClerk说明.md) 的续篇。
>
> **前提**：建议先读完 raftClerk 那份，建立"客户端契约"的心智模型，再进这里。

## 目录

- [1. 模块定位](#1-模块定位)
- [2. 文件清单](#2-文件清单)
- [3. 读代码前必须搞懂的一个概念](#3-读代码前必须搞懂的一个概念)
- [4. Raft 的全部状态](#4-raft-的全部状态)
- [5. 三条控制流](#5-三条控制流)
- [6. 一次写请求的完整生命周期](#6-一次写请求的完整生命周期)
- [7. RPC 入口：继承生成的 Service](#7-rpc-入口继承生成的-service)
- [8. 选举](#8-选举)
- [9. 日志复制](#9-日志复制)
- [10. commitIndex 的推进](#10-commitindex-的推进)
- [11. 快照](#11-快照)
- [12. 持久化](#12-持久化)
- [13. kvServer 层](#13-kvserver-层)
- [14. 已知疑点](#14-已知疑点)
- [15. 与工业实现的差距](#15-与工业实现的差距)

---

## 1. 模块定位

```
raft.cpp / raft.h          ← 共识算法本体（~1290 行）
kvServer.cpp / kvServer.h  ← 状态机 + 去重 + 快照策略（~580 行）
Persister.cpp / .h         ← 落盘（~170 行）
raftRpcUtil.cpp / .h       ← 节点间出站 stub（~68 行）
ApplyMsg.h                 ← Raft → KvServer 的消息结构
```

三种角色关系：

```
        KvServer ──── Start(op) ────▶ Raft
            ▲                          │
            │                    AppendEntries / RequestVote
       applyChan ◀── ApplyMsg ────┘    │
       (LockQueue)                      ▼
                                   其他节点的 Raft
```

- **`Raft` 继承 `raftRpcProctoc::raftRpc`**（公有继承，`raft.h:34`）—— 同一个对象既对外提供 RPC 服务，又持有全部状态。被重写的 `google::protobuf::RpcController` 风格方法就是 RPC 入口。
- **`KvServer` 继承 `raftKVRpcProctoc::kvServerRpc`**（**私有**继承，`kvServer.h:25`）—— 注意与 Raft 不一致，见[疑点 2](#14-已知疑点)。
- 两者**共享同一个 `applyChan`**（`LockQueue<ApplyMsg>`）和**同一个 `Persister`**。

---

## 2. 文件清单

| 文件 | 行数 | 作用 |
|---|---:|---|
| `include/raft.h` | 168 | `Raft` 类声明、`BoostPersistRaftNode` 序列化结构 |
| `raft.cpp` | 1121 | 全部共识逻辑 |
| `include/kvServer.h` | 134 | `KvServer` 声明、自身序列化 |
| `kvServer.cpp` | 450 | 服务层 |
| `include/Persister.h` | 51 | 落盘接口 |
| `Persister.cpp` | 121 | 文件读写实现 |
| `include/raftRpcUtil.h` | 31 | 出站 stub 声明 |
| `raftRpcUtil.cpp` | 37 | stub 实现 |
| `include/ApplyMsg.h` | 26 | Raft→KvServer 消息 |

---

## 3. 读代码前必须搞懂的一个概念

**这是全模块最容易卡住的地方：日志有两个坐标系。**

| 概念 | 含义 | 范围 |
|---|---|---|
| **逻辑 index**（logIndex） | 日志在 Raft 全局序列中的编号 | 从 `m_lastSnapshotIncludeIndex + 1` 开始 |
| **物理下标**（slice index） | 日志在 `m_logs` 这个 `vector` 中的位置 | 从 `0` 开始 |

因为快照会**丢弃 `m_logs` 的前缀**（`Snapshot` 里 `m_logs = trunckedLogs`），所以两者差了一个 `m_lastSnapshotIncludeIndex`：

```cpp
// raft.cpp:750
int Raft::getSlicesIndexFromLogIndex(int logIndex) {
  myAssert(logIndex > m_lastSnapshotIncludeIndex, ...);
  ...
  return logIndex - m_lastSnapshotIncludeIndex - 1;   // ← 唯一的换算公式
}
```

于是有了三条使用规则：

1. **任何 `m_logs[i]` 的访问，都必须先经过 `getSlicesIndexFromLogIndex()`。**
2. **`getLogTermFromLogIndex(index)` 能同时处理快照内的 index**：等于 `m_lastSnapshotIncludeIndex` 时返回 `m_lastSnapshotIncludeTerm`，否则查 `m_logs`（`raft.cpp:729`）。
3. **`getSlicesIndexFromLogIndex` 不能传 `<= m_lastSnapshotIncludeIndex` 的值**，会断言失败。

没有这个认识，读 `AppendEntries1` 里那一堆 `getSlicesIndexFromLogIndex` 会完全迷失。

---

## 4. Raft 的全部状态

`raft.h:35-70`，对照 Raft 论文 Figure 2：

**持久化状态**（崩溃后必须恢复，`BoostPersistRaftNode` 里序列化）：

| 字段 | 说明 |
|---|---|
| `m_currentTerm` | 当前任期 |
| `m_votedFor` | 本任期投给了谁，`-1` 表示没投 |
| `m_logs` | 日志条目（`vector<LogEntry>`） |
| `m_lastSnapshotIncludeIndex` | 快照包含到的逻辑 index |
| `m_lastSnapshotIncludeTerm` | 上述 index 的 term |

**易失状态**：

| 字段 | 说明 |
|---|---|
| `m_commitIndex` | 已提交的最高 index |
| `m_lastApplied` | 已汇报给状态机的最高 index |
| `m_status` | `Follower` / `Candidate` / `Leader` |
| `m_nextIndex[]` | **仅 leader**：给每个 follower 的下一个待发送 index |
| `m_matchIndex[]` | **仅 leader**：已知每个 follower 已复制的最高 index |

**本实现特有的两个**：

| 字段 | 说明 |
|---|---|
| `m_lastResetElectionTime` | 选举定时器基准时间，收到 AE / 投票时重置 |
| `m_lastResetHearBeatTime` | 心跳定时器基准时间 |

> `m_nextIndex` / `m_matchIndex` 初始化为 `0`，成为 leader 时统一设为 `lastLogIndex + 1` 和 `0`（`raft.cpp:816-819`）。`doHeartBeat` 里有 `myAssert(m_nextIndex[i] >= 1)` 兜底。

---

## 5. 三条控制流

`Raft::init`（`raft.cpp:1027-1037`）启动三个循环：

```cpp
m_ioManager->scheduler([this]() { this->leaderHearBeatTicker(); });  // 协程
m_ioManager->scheduler([this]() { this->electionTimeOutTicker(); }); // 协程
std::thread t3(&Raft::applierTicker, this);                          // 独立线程
t3.detach();
```

前两个跑在 `monsoon::IOManager` 协程池里（线程池大小由 `config.h` 的 `FIBER_THREAD_NUM` 决定），第三个是独立 `std::thread`。

### 5.1 `electionTimeOutTicker`（`raft.cpp:313`）

```
while (true) {
    while (is_leader) usleep(HeartBeatTimeout);        // leader 不参与选举，空转
    拿到锁，算出 suitableSleepTime = 随机选举超时 + m_lastResetElectionTime - now
    usleep(suitableSleepTime)
    if (m_lastResetElectionTime - wakeTime > 0) continue;   // 睡眠期间被重置了 → 没超时
    doElection();
}
```

**关键设计**：`suitableSleepTime` 是"睡到 `上次重置时间 + 随机超时`"。醒来后检查 `m_lastResetElectionTime` 是否变过——变过说明这期间收到了心跳/投票，定时器被续期了，于是重算再睡。这是**用睡眠时长模拟定时器**的做法，避免了每轮都拿锁。

`getRandomizedElectionTimeout()` 返回 `[300, 500] ms` 的随机值（`config.h:14-15`），随机化是为了避免所有 follower 同时超时导致选票瓜分。

### 5.2 `leaderHearBeatTicker`（`raft.cpp:480`）

结构同上，但：
- 非 leader 时 `usleep(1000 * HeartBeatTimeout)` 等待；
- 超时后调 `doHeartBeat()`——它是**心跳和日志复制的统一入口**。

### 5.3 `applierTicker`（`raft.cpp:155`）

```
while (true) {
    拿锁 → getApplyLogs() 把 [m_lastApplied+1, m_commitIndex] 之间的日志转成 ApplyMsg
    放锁 → applyChan->Push(msg)
    sleepNMilliseconds(ApplyInterval);   // 10ms
}
```

**这是 Raft 与状态机之间唯一的通道。** 注意它把"取日志"和"投递"分开在锁内外，避免持锁阻塞。

---

## 6. 一次写请求的完整生命周期

以 `Put` 为例，把 [raftClerk 说明第 8 节](../raftClerk/raftClerk说明.md#8-线性一致性从哪来) 那张表落实到代码：

```
① Clerk::PutAppend  ──RPC──▶  KvServer::PutAppend(controller,...)   kvServer.cpp:365
                                    │ 转发到真正实现
②                               KvServer::PutAppend(args, reply)    kvServer.cpp:212
                                    │ 构造 Op
③                               Raft::Start(op, &idx, ...)          raft.cpp:947
                                    │ ├ 不是 leader → ErrWrongLeader
                                    │ └ 是 leader → 追加到 m_logs，返回 index
④                               在 waitApplyCh[idx] 注册 LockQueue，解锁等待
                                    │
⑤         (AppendEntries 复制到多数派 → commitIndex 推进)
                                    │
⑥                               applierTicker → applyChan->Push(ApplyMsg)
                                    │
⑦         ReadRaftApplyCommandLoop  ──Pop──▶  GetCommandFromRaft    kvServer.cpp:166
                                    │ ├ 去重检查 ifRequestDuplicate
                                    │ ├ 执行 ExecutePutOpOnKVDB → SkipList
                                    │ └ SendMessageToWaitChan(op, index)
                                    │
⑧                               ④ 中阻塞的线程被唤醒，比对 clientId/requestId → 回 OK
```

**三个衔接点，逐个看：**

**`Raft::Start`（`raft.cpp:947`）** —— 只做三件事：检查是不是 leader、追加日志、`persist()`。

```cpp
if (m_status != Leader) { *isLeader = false; return; }
newLogEntry.set_logterm(m_currentTerm);
newLogEntry.set_logindex(getNewCommandIndex());
m_logs.emplace_back(newLogEntry);
persist();
*newLogIndex = newLogEntry.logindex();
```

注意 `isLeader` 是**双重检查**：这里是 Start 时的检查，`KvServer::PutAppend` 收到结果后还会再检查一次。

**`waitApplyCh`（`kvServer.h:38`）** —— `map<raftIndex, LockQueue<Op>*>`。这是"RPC 线程等自己的日志被 apply"的会合点。注册、等待、清理三段：

```cpp
// 注册（kvServer.cpp:239-244）
m_mtx.lock();
if (waitApplyCh.find(raftIndex) == waitApplyCh.end())
    waitApplyCh.insert({raftIndex, new LockQueue<Op>()});
auto chForRaftIndex = waitApplyCh[raftIndex];
m_mtx.unlock();                                   // ← 立刻解锁，不能持锁等待

// 等待（带超时）
if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) { ... }

// 清理（kvServer.cpp:273-278）
m_mtx.lock();
auto tmp = waitApplyCh[raftIndex];
waitApplyCh.erase(raftIndex);
delete tmp;
m_mtx.unlock();
```

**`SendMessageToWaitChan`（`kvServer.cpp:324`）** —— apply 线程把 op 塞进对应 index 的队列，唤醒等待者。若该 index 无人等待（比如请求已超时返回），直接 `return false`。

---

## 7. RPC 入口：继承生成的 Service

这是理解本模块结构的钥匙。`Raft` 公有继承 `raftRpcProctoc::raftRpc`，重写三个方法：

```cpp
// raft.cpp:926
void Raft::AppendEntries(controller, request, response, done) {
  AppendEntries1(request, response);   // ← 转发到真正的实现
  done->Run();
}
void Raft::InstallSnapshot(...) { InstallSnapshot(request, response); done->Run(); }
void Raft::RequestVote(...)     { RequestVote(request, response);     done->Run(); }
```

**为什么要有这一层转发？** 因为 protobuf 生成的方法签名带 `RpcController*` 和 `Closure*` 参数，而内部实现只想拿 `args`/`reply`。

命名规律：`AppendEntries` = RPC 入口（带 controller），`AppendEntries1` = 内部实现。**只有 `AppendEntries` 有这个 `1` 后缀**，`RequestVote` / `InstallSnapshot` 是重载（同名不同参）。读代码时注意 `RequestVote` 有两个：`raft.cpp:611`（内部，两参数）和 `raft.cpp:941`（RPC 入口，四参数）。

三个入口**全部持 `m_mtx`**：
- `AppendEntries1` 用 `std::lock_guard`（`raft.cpp:9`）
- `RequestVote` 用 `std::lock_guard` + `DEFER { persist(); }`（`raft.cpp:612-618`）
- `InstallSnapshot` 用 `m_mtx.lock()` + `DEFER { m_mtx.unlock(); }`（`raft.cpp:422-423`）

> 两种 `DEFER` 风格混用是代码演进留下的痕迹，功能等价，但 `AppendEntries1` 里 `DEFER { persist(); }` 和 `std::lock_guard` 的组合需要想一下析构顺序才敢说是安全的。

---

## 8. 选举

### `doElection`（`raft.cpp:202`）

由 `electionTimeOutTicker` 在超时时调用，持锁：

```cpp
if (m_status == Leader) return;          // leader 不选举
m_status = Candidate;
m_currentTerm += 1;
m_votedFor = m_me;                       // 给自己投票
persist();                               // 任期和投票必须落盘
std::shared_ptr<int> votedNum = std::make_shared<int>(1);   // 自己这一票
m_lastResetElectionTime = now();

for (每个 peer) {                        // 并行发起
    std::thread t(&Raft::sendRequestVote, this, i, args, reply, votedNum);
    t.detach();
}
```

### `sendRequestVote`（`raft.cpp:761`）

每个对端一个 detached 线程，共享一个 `shared_ptr<int> votedNum` 统计票数：

```cpp
bool ok = m_peers[server]->RequestVote(args.get(), reply.get());
if (!ok) return ok;                      // 网络失败直接放弃

std::lock_guard lg(m_mtx);
// ① 无论何时收到响应都要检查 term
if (reply->term() > m_currentTerm) {     // 三变：身份、term、投票
    m_status = Follower; m_currentTerm = reply->term(); m_votedFor = -1;
    persist();  return true;
} else if (reply->term() < m_currentTerm) return true;

// ② 计票
if (!reply->votegranted()) return true;
*votedNum = *votedNum + 1;
if (*votedNum >= m_peers.size() / 2 + 1) {
    *votedNum = 0;                       // 防止重复触发
    if (m_status == Leader) myAssert(false, "同一个term当两次领导");
    m_status = Leader;
    for (i...) { m_nextIndex[i] = lastLogIndex + 1; m_matchIndex[i] = 0; }
    std::thread t(&Raft::doHeartBeat, this);  t.detach();   // 立即宣告
    persist();
}
```

**注意 `*votedNum` 是数据竞争**，见[疑点 4](#14-已知疑点)。

### `RequestVote` 服务端（`raft.cpp:611`）

```cpp
if (args->term() < m_currentTerm)      → 拒绝（Expire）
if (args->term() > m_currentTerm)      → 三变，然后继续处理
myAssert(args->term() == m_currentTerm);

if (!UpToDate(args->lastlogindex(), args->lastlogterm()))
                                       → 拒绝（日志太旧）
if (m_votedFor != -1 && m_votedFor != args->candidateid())
                                       → 拒绝（本任期已投给别人）
else                                   → 授票，重置选举定时器
```

**`UpToDate`（`raft.cpp:685`）—— 选举安全性的核心**：

```cpp
return term > lastTerm || (term == lastTerm && index >= lastIndex);
```

"候选人的日志必须**至少和我一样新**，我才投票"。这一条保证了当选者一定含有所有已提交的日志（Raft 论文 §5.4.1）。

---

## 9. 日志复制

### `doHeartBeat`（`raft.cpp:247`）

心跳与日志复制的**统一入口**，leader 定时触发：

```cpp
for (每个 peer) {
    // 分支 A：该 follower 需要的日志已被快照丢弃 → 发快照
    if (m_nextIndex[i] <= m_lastSnapshotIncludeIndex) {
        std::thread t(&Raft::leaderSendSnapShot, this, i); t.detach();
        continue;
    }
    // 分支 B：正常发 AE
    getPrevLogInfo(i, &preLogIndex, &PrevLogTerm);
    appendEntriesArgs->set_prevlogindex(preLogIndex);
    appendEntriesArgs->set_prevlogterm(PrevLogTerm);
    appendEntriesArgs->clear_entries();
    // 从 preLogIndex 之后一直填到日志末尾
    for (j = getSlicesIndexFromLogIndex(preLogIndex)+1; j < m_logs.size(); ++j)
        *appendEntriesArgs->add_entries() = m_logs[j];
    appendEntriesArgs->set_leadercommit(m_commitIndex);

    std::thread t(&Raft::sendAppendEntries, this, i, args, reply, appendNums);
    t.detach();
}
m_lastResetHearBeatTime = now();
```

三个要点：
- **每个 peer 一个 detached 线程**，并发发送。
- **心跳就是空 entries 的 AE** —— 没有单独的 heartbeat RPC。
- `getPrevLogInfo`（`raft.cpp:393`）按 `m_nextIndex[server]` 算出要发的 `prevLogIndex/prevLogTerm`；若 `nextIndex == lastSnapshotIncludeIndex + 1`，prev 就是快照的那一项。

### `sendAppendEntries`（`raft.cpp:828`）

处理响应，是**回退优化的落点**：

```cpp
if (reply->term() > m_currentTerm) { 三变;  return; }
if (m_status != Leader) return;              // 中间可能被降级

if (!reply->success()) {
    // 日志不匹配 → 回退 nextIndex
    if (reply->updatenextindex() != -100)     // -100 是特殊值，表示"别回退"
        m_nextIndex[server] = reply->updatenextindex();
} else {
    m_matchIndex[server] = max(m_matchIndex[server], prevlogindex + entries_size);
    m_nextIndex[server] = m_matchIndex[server] + 1;
    if (++(*appendNums) >= 1 + m_peers.size()/2) {
        *appendNums = 0;
        // 只有【当前 term】的日志被多数派确认才推进 commitIndex
        if (entries_size > 0 && last_entry.logterm() == m_currentTerm)
            m_commitIndex = max(m_commitIndex, prevlogindex + entries_size);
    }
}
```

**`m_matchIndex` 用 `std::max` 而非直接赋值**：因为心跳会重复发送同一段日志，直接赋值会让 matchIndex 倒退（`raft.cpp:885` 的注释专门解释了这点）。

### `AppendEntries1` 服务端（`raft.cpp:8`）

全模块最长最绕的函数，按顺序：

```cpp
① if (args->term() < m_currentTerm) → 拒绝，返回 updatenextindex = -100
② DEFER { persist(); }
③ if (args->term() > m_currentTerm) → 三变（身份/term/votedFor）
④ m_status = Follower;                  // candidate 收到同 term 的 AE 要转变
⑤ m_lastResetElectionTime = now();      // 重置选举定时器

⑥ 日志一致性检查：
   if (args->prevlogindex() > getLastLogIndex())
       → 拒绝，updatenextindex = lastLogIndex + 1
   else if (args->prevlogindex() < m_lastSnapshotIncludeIndex)
       → 拒绝，updatenextindex = m_lastSnapshotIncludeIndex + 1
       ⚠️ 这里没有 return，见【疑点 1】

⑦ if (matchLog(prevlogindex, prevlogterm)) {
       逐个 entries：
         - index > lastLogIndex  → push_back
         - index 相同但 term 不同 → 覆盖
         - index 相同 term 相同但 command 不同 → myAssert(false)  // 违反 Log Matching
       推进 m_commitIndex = min(leadercommit, lastLogIndex)
       reply->success = true
   } else {
       // 冲突回退优化：往前找到 term 矛盾的第一个位置
       for (index = prevlogindex; index >= lastSnapshotIncludeIndex; --index)
           if (getLogTermFromLogIndex(index) != getLogTermFromLogIndex(prevlogindex)) {
               updatenextindex = index + 1;  break;
           }
       reply->success = false;
   }
```

**本实现最重要的一个偏离论文的地方**：论文说"冲突时直接截断 follower 的日志"，这里改成**逐条比对后覆盖**（`raft.cpp:71-96`）。原因写在注释里：

> 不能直接截断，必须一个一个检查，因为发送来的 log 可能是之前的，直接截断可能导致"取回"已经在 follower 日志中的条目。

这是为了容忍 **RPC 乱序 / 延迟到达**：过于陈旧的 AE 不该把新日志截掉。代价是代码复杂，而且引入了一个 `myAssert(false)` 的崩溃路径（见疑点 1 旁注）。

**回退优化（`raft.cpp:126-150`）**：不匹配时不是简单地 `nextIndex--`，而是一次跳过整个冲突 term，算出 `updatenextindex` 回传给 leader，减少 RPC 轮数。上限是"每个 term 一次往返"而不是"每个 index 一次"。

---

## 10. commitIndex 的推进

**本实现有两套推进逻辑，但只有一套在跑。**

| 位置 | 状态 |
|---|---|
| `Raft::leaderUpdateCommitIndex()`（`raft.cpp:562`） | **死代码** —— 声明了、实现了，但全项目没有任何调用点 |
| `sendAppendEntries` 中的内联逻辑（`raft.cpp:894-915`） | 实际生效 |

内联版本：

```cpp
if (*appendNums >= 1 + m_peers.size() / 2) {     // 多数派确认
    *appendNums = 0;
    if (args->entries_size() > 0 &&
        args->entries(args->entries_size()-1).logterm() == m_currentTerm) {
        m_commitIndex = std::max(m_commitIndex, args->prevlogindex() + args->entries_size());
    }
}
```

对照 `leaderUpdateCommitIndex` 的原版逻辑（`matchIndex` 统计 + `getLogTermFromLogIndex(index) == m_currentTerm`），两者思路一致，但**统计口径不同**：

- 内联版用 `appendNums`（本次 AE 得到的成功数）**近似**多数派，且只检查**当前这一批**的最后一个 entry 的 term；
- 原版用 `matchIndex[]` 数组精确统计，对每个候选 index 单独检查 term。

**差别**：内联版在 `entries_size() == 0`（纯心跳）时永远不会推进 commitIndex，只能靠"带着新日志的 AE"来推进。多数情况下够用，但严格说不如原版严谨——原版能在心跳时也把之前 term 的日志提交掉。这解释了为什么 `Start` 的注释里写着"新的命令来了不会直接执行，而是等待 leader 的心跳触发"。

**两条铁律，两处都遵守了**：
1. 只有多数派确认才能提交（`sum >= size/2 + 1` / `appendNums >= 1 + size/2`）；
2. **只有当前 term 的日志才能推进 commitIndex**（Raft 论文 §5.4.2）——防止旧 term 的日志被"误提交"。

---

## 11. 快照

三个方向，分别对应三个函数：

| 场景 | 函数 |
|---|---|
| 服务层告诉 Raft "我打了快照" | `Raft::Snapshot`（`raft.cpp:1087`） |
| leader 主动给落后的 follower 发 | `Raft::leaderSendSnapShot`（`raft.cpp:528`） |
| follower 接收 | `Raft::InstallSnapshot`（`raft.cpp:420`） |

### `Raft::Snapshot`（服务层发起）

```cpp
if (m_lastSnapshotIncludeIndex >= index || index > m_commitIndex) return;  // 合法性
int newLastSnapshotIncludeTerm = m_logs[getSlicesIndexFromLogIndex(index)].logterm();
// 保留 index 之后的所有日志
for (int i = index + 1; i <= getLastLogIndex(); i++)
    trunckedLogs.push_back(m_logs[getSlicesIndexFromLogIndex(i)]);
m_lastSnapshotIncludeIndex = index;
m_logs = trunckedLogs;                      // ← 日志前缀被丢弃
m_commitIndex = std::max(m_commitIndex, index);
m_lastApplied = std::max(m_lastApplied, index);
m_persister->Save(persistData(), snapshot);
```

**`index > m_commitIndex` 才允许打快照**——只能对已提交的状态打快照，否则崩溃后无法恢复。

### `leaderSendSnapShot`（leader 发起）

```cpp
m_mtx.lock();
读 m_lastSnapshotIncludeIndex/Term、m_persister->ReadSnapshot() 到 args;   // 必须在锁内读快照元信息
m_mtx.unlock();
bool ok = m_peers[server]->InstallSnapshot(&args, &reply);   // RPC 在锁外
m_mtx.lock();
DEFER { m_mtx.unlock(); };
if (reply->term() > m_currentTerm) → 三变;
m_matchIndex[server] = args.lastsnapshotincludeindex();
m_nextIndex[server] = m_matchIndex[server] + 1;
```

**"锁内取数据、锁外发 RPC"** 是全模块反复出现的模式——因为 RPC 可能耗时几百毫秒，持锁会阻塞整个节点。

### `InstallSnapshot`（follower 接收）

```cpp
if (args->term() < m_currentTerm) → 拒绝;
if (args->term() > m_currentTerm) → 三变 + persist();
m_status = Follower;  m_lastResetElectionTime = now();

if (args->lastsnapshotincludeindex() <= m_lastSnapshotIncludeIndex) return;  // 过期快照

// 截断日志：长了截断，短了清空（getSlicesIndexFromLogIndex 不能传不存在的 index）
if (lastLogIndex > args->lastsnapshotincludeindex())
    m_logs.erase(begin, begin + getSlicesIndexFromLogIndex(args->lastsnapshotincludeindex()) + 1);
else
    m_logs.clear();
m_commitIndex = max(m_commitIndex, index);
m_lastApplied = max(m_lastApplied, index);
m_lastSnapshotIncludeIndex/Term = ...;

// 通知状态机（新线程，避免持锁阻塞）
ApplyMsg msg; msg.SnapshotValid = true; msg.Snapshot = args->data(); ...
std::thread t(&Raft::pushMsgToKvServer, this, msg);  t.detach();
m_persister->Save(persistData(), args->data());
```

**`ApplyMsg` 有两种形态**（`ApplyMsg.h`），靠两个 bool 区分：

| 形态 | `CommandValid` | `SnapshotValid` | 处理者 |
|---|---|---|---|
| 普通命令 | `true` | `false` | `KvServer::GetCommandFromRaft` |
| 快照 | `false` | `true` | `KvServer::GetSnapShotFromRaft` |

### `CondInstallSnapshot` —— 一个被架空的机制

论文里，follower 收到快照后要先调 `CondInstallSnapshot` **协商**：如果快照已过期或状态机已推进得更远，就拒绝安装。

本实现：

```cpp
// raft.cpp:177
bool Raft::CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot) {
  return true;                      // ← 直接返回 true
  //// Your code here (2D).         // ← 全部逻辑被注释掉
  ...
}
```

而 `InstallSnapshot` **已经自己改完了所有状态**（`m_logs`、`m_commitIndex`、`m_lastApplied`、`m_lastSnapshotIncludeIndex`）并向 `applyChan` 推了消息，`KvServer::GetSnapShotFromRaft`（`kvServer.cpp:350`）再调这个恒返回 `true` 的函数当"检查"。**协商机制完全失效**，只剩下形式。

---

## 12. 持久化

### `Persister`（`Persister.cpp`）

两个文件，按节点编号命名：

```
raftstatePersist<i>.txt     ← Raft 状态（term / votedFor / logs / 快照元信息）
snapshotPersist<i>.txt      ← 状态机快照字节流
```

| 方法 | 行为 |
|---|---|
| `SaveRaftState(data)` | `clearRaftState()`（truncate）后写，`m_raftStateSize = data.size()` |
| `Save(state, snapshot)` | 同时清空两个文件并写入 |
| `ReadRaftState()` / `ReadSnapshot()` | 打开、`>>` 读取、关闭 |

**注意 `m_raftStateSize` 是"文件当前大小"而非累计值**——因为 `clearRaftState()` 每次都把它置 0 再 truncate。`GetRaftStateSize()` 就是靠它判断是否该打快照的。

**`ReadSnapshot` 里有个别扭的写法**（`Persister.cpp:16-33`）：

```cpp
if (m_snapshotOutStream.is_open()) m_snapshotOutStream.close();
DEFER { m_snapshotOutStream.open(m_snapshotFileName); };   // 读完再把写流开回来
std::fstream ifs(m_snapshotFileName, std::ios_base::in);
```

因为 `m_snapshotOutStream` 一直开着，不关掉的话读到的可能是缓冲区里还没落盘的内容。用 `DEFER` 把写流恢复，属于临时绕路。

### `persist` / `readPersist`（`raft.cpp:603, 1065`）

```cpp
void Raft::persist() {
  auto data = persistData();           // BoostPersistRaftNode → boost text archive
  m_persister->SaveRaftState(data);
}

std::string Raft::persistData() {
  BoostPersistRaftNode n;
  n.m_currentTerm = m_currentTerm;
  n.m_votedFor = m_votedFor;
  n.m_lastSnapshotIncludeIndex/Term = ...;
  for (auto& item : m_logs) n.m_logs.push_back(item.SerializeAsString());   // ← 每条日志单独 protobuf 序列化
  std::stringstream ss; boost::archive::text_oarchive oa(ss); oa << n;
  return ss.str();
}
```

**持久化的粒度是"整个状态"**：每次 `persist()` 都重新序列化**全部** `m_logs`，truncate 后整体重写。不是追加式 WAL。这就是为什么快照如此重要——没有它，日志无限增长，每次持久化都是 O(日志总长)。

`init` 里的恢复（`raft.cpp:1016-1020`）：

```cpp
readPersist(m_persister->ReadRaftState());
if (m_lastSnapshotIncludeIndex > 0)
    m_lastApplied = m_lastSnapshotIncludeIndex;
    // rf.commitIndex = rf.lastSnapshotIncludeIndex   ← 注释掉了，见疑点 6
```

---

## 13. kvServer 层

### 构造函数（`kvServer.cpp:377`）—— 启动时序全貌

```cpp
① 起一个 detached 线程跑 RpcProvider::Run(m_me, port)   // 发布两个 service：this 和 m_raftNode
② sleep(6);                                            // 等所有节点都开好 RPC 服务
③ 读配置，连接所有其他节点（自己那槽是 nullptr）
④ sleep(ipPortVt.size() - me);                         // 再等，错开启动
⑤ m_raftNode->init(servers, m_me, persister, applyChan);
⑥ 从 persister 读快照恢复状态机
⑦ 启动 ReadRaftApplyCommandLoop 并 join（永不返回）
```

**第 ⑦ 步的 join 意味着构造函数永不返回。** `ReadRaftApplyCommandLoop` 跑在独立线程上、无限循环，`join` 让构造函数阻塞在那里，进程得以继续跑 RPC 事件循环。这是一个不太常规的设计——正常应该让对象构造完就返回、由外部启动循环。

第 ①②④ 步的 `sleep` 是**脆弱的启动同步**：靠固定睡眠而非握手/重试来保证"所有节点都起来了"。`sleep(6)` 和 `sleep(nodeCount - me)` 这两个魔数决定了集群能否正确启动。

### `PutAppend`（`kvServer.cpp:212`）

```cpp
Op op{Operation, Key, Value, ClientId, RequestId};
m_raftNode->Start(op, &raftIndex, &_, &isleader);
if (!isleader) { reply->set_err(ErrWrongLeader); return; }   // ← ErrWrongLeader 的真正来源

注册 waitApplyCh[raftIndex];
if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {
    // 超时
    if (ifRequestDuplicate(op.ClientId, op.RequestId))
        reply->set_err(OK);              // 重复请求 → 早就执行过了，直接回 OK
    else
        reply->set_err(ErrWrongLeader);  // 让 Clerk 换节点重试
} else {
    // 等到了 apply
    if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId)
        reply->set_err(OK);
    else
        reply->set_err(ErrWrongLeader);  // ← 日志被覆盖了（leader 变更），必须让客户端重试
}
清理 waitApplyCh[raftIndex];
```

**两道防线**，缺一不可：
- 超时分支用 `ifRequestDuplicate` —— 请求可能已经执行了，只是响应没回来；
- 成功分支比对 `clientId/requestId` —— 防止"我等的 index 上最终坐着别人的日志"（leader 变更导致日志被覆盖）。

### `Get`（`kvServer.cpp:79`）

结构与 `PutAppend` 几乎一样，差别在超时分支：

```cpp
if (!chForRaftIndex->timeOutPop(...)) {
    m_raftNode->GetState(&_, &isLeader);
    if (ifRequestDuplicate(op.ClientId, op.RequestId) && isLeader) {
        // 已提交过的 Get 可以本地重放，不违反线性一致性
        ExecuteGetOpOnKVDB(op, &value, &exist);
        ...
    } else reply->set_err(ErrWrongLeader);
}
```

注释给的论证是：**"如果超时，代表 raft 集群不保证已经 commit 该日志，但如果这个 Get 请求是已经提交过的，那它之前必然被 apply 过，本地状态机中有对应状态，可以安全重放。"**

注意 `Get` **也走 `Raft::Start`**——它是线性一致读，不是本地读。这一点和外界"读不该写日志"的直觉相悖，见[疑点 8](#14-已知疑点)。

### 去重表 `ifRequestDuplicate`（`kvServer.cpp:200`）

```cpp
bool KvServer::ifRequestDuplicate(std::string ClientId, int RequestId) {
  std::lock_guard lg(m_mtx);
  if (m_lastRequestId.find(ClientId) == m_lastRequestId.end()) return false;
  return RequestId <= m_lastRequestId[ClientId];     // ← 单调序列，用 <=
}
```

`m_lastRequestId[clientId]` 记录该客户端**最后执行的** RequestId。因为 Clerk 的 `m_requestId` 单调递增，所以 `RequestId <= 记录值` 就意味着重复。

**这是整个系统线性一致性的最后一道闸门**——把客户端的"至少一次"投递变成"恰好一次"效果。

更新点在三处执行函数结尾：`ExecutePutOpOnKVDB` / `ExecuteAppendOpOnKVDB` / `ExecuteGetOpOnKVDB` 都写了 `m_lastRequestId[op.ClientId] = op.RequestId`（`kvServer.cpp:33, 53, 70`）。

### `GetCommandFromRaft`（`kvServer.cpp:166`）—— apply 的核心

```cpp
Op op; op.parseFromString(message.Command);
if (message.CommandIndex <= m_lastSnapShotRaftLogIndex) return;   // 快照已覆盖

if (!ifRequestDuplicate(op.ClientId, op.RequestId)) {             // 去重闸门
    if (op.Operation == "Put")    ExecutePutOpOnKVDB(op);
    if (op.Operation == "Append") ExecuteAppendOpOnKVDB(op);
    // 注意：没有 "Get" 分支！
}

if (m_maxRaftState != -1)
    IfNeedToSendSnapShotCommand(message.CommandIndex, 9);

SendMessageToWaitChan(op, message.CommandIndex);                  // 唤醒等待者
```

**Get 没有执行分支**，因为 Get 不改状态机——它的值是在 RPC 线程里直接读 SkipList 拿到的（`ExecuteGetOpOnKVDB`）。apply 循环对 Get 只做两件事：推进去重表、唤醒等待者。

**但这也意味着：`m_lastRequestId` 对 Get 的更新发生在 RPC 线程，而非 apply 线程。** 见[疑点 7](#14-已知疑点)。

### 快照策略

```cpp
// kvServer.cpp:342
void KvServer::IfNeedToSendSnapShotCommand(int raftIndex, int proportion) {
  if (m_raftNode->GetRaftStateSize() > m_maxRaftState / 10.0) {
      auto snapshot = MakeSnapShot();
      m_raftNode->Snapshot(raftIndex, snapshot);
  }
}
```

**`proportion` 参数完全没用上**——调用点传 9，函数体硬编码 `/10.0`。见[疑点 3](#14-已知疑点)。

`m_maxRaftState` 由构造函数参数决定，示例启动器传的是 **`500`**（`example/raftCoreExample/raftKvDB.cpp:58`）：

```cpp
auto kvServer = new KvServer(i, 500, configFileName, port);
```

于是快照阈值是 `500 / 10 = 50` 字节。而 `m_raftStateSize` 是 `persistData()` 的字节长度，光是 term/votedFor/快照元信息加几条日志就远超 50 字节——**这意味着几乎每 apply 一条命令就会触发一次快照**。这个数字明显是调试用的，不是有意的生产配置。

**快照内容**（`kvServer.h:114`）：

```cpp
std::string getSnapshotData() {
    m_serializedKVData = m_skipList.dump_file();     // 跳表整体转字符串
    std::stringstream ss; boost::archive::text_oarchive oa(ss);
    oa << *this;                                      // 序列化 m_serializedKVData + m_lastRequestId
    m_serializedKVData.clear();                       // 序列化完清掉，避免重复占用内存
    return ss.str();
}
```

`KvServer` 的 `serialize()` 只序列化两个字段：`m_serializedKVData`（跳表数据）和 `m_lastRequestId`（**去重表必须进快照**，否则恢复后无法去重）。`m_kvDB` 被注释掉了——那是早期用 `unordered_map` 的实现残留。

**这是"快照格式由服务层定义，Raft 只负责传递字节流"的体现**（`kvServer.cpp:299-302` 的注释专门说明了这一点）。

---

## 14. 已知疑点

> ⚠️ 以下均为**静态阅读**发现，未在运行集群中验证。按严重程度排序。
>
> **更新（已修复）**：本节的疑点 1（`AppendEntries1` 缺 `return` 导致 `exit`）、
> 疑点 6（崩溃恢复不读 `commitIndex`，且 `Persister` 会把落盘文件清空，恢复实际从未生效）
> 已经确认并修复。完整的缺陷清单、修复内容与实测证据见
> [benchmark/README.md](../../benchmark/README.md#85-压测中发现并已修复的缺陷)。

### 1. `AppendEntries1` 快照分支缺少 `return` —— 会导致进程 `exit`

`raft.cpp:53-62`：

```cpp
} else if (args->prevlogindex() < m_lastSnapshotIncludeIndex) {
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(m_lastSnapshotIncludeIndex + 1);
    //  DPrintf(...)
    // return            ← 被注释掉了！
}
```

不 `return` 就会落到第 65 行的 `matchLog(args->prevlogindex(), args->prevlogterm())`：

```cpp
// raft.cpp:592
myAssert(logIndex >= m_lastSnapshotIncludeIndex && ...);
```

而此处 `logIndex = prevlogindex < m_lastSnapshotIncludeIndex`，断言必然失败。`myAssert` 的实现是（`util.cpp:8-13`）：

```cpp
void myAssert(bool condition, std::string message) {
  if (!condition) { std::cerr << "Error: " << message << std::endl; std::exit(EXIT_FAILURE); }
}
```

**直接 `exit` 掉整个进程**。触发条件是"leader 发来的 AE 的 prevLogIndex 落在 follower 已快照的范围内"——即 follower 的快照比 leader 认为的更新，这本是正常的落后/恢复场景。

### 2. `KvServer` 是**私有**继承，`Raft` 是**公有**继承

```cpp
class Raft    : public  raftRpcProctoc::raftRpc      // raft.h:34
class KvServer :         raftKVRpcProctoc::kvServerRpc  // kvServer.h:25，默认 private
```

`provider.NotifyService(this)` 传的是 `void*`，私有继承在这里能编译通过、也能跑，但语义不一致：同一份代码里两个 service 对象用了不同的继承方式。如果 `mprpc` 框架内部做了 `dynamic_cast` 或类型检查就会出问题。

### 3. `IfNeedToSendSnapShotCommand` 的参数 `proportion` 未使用

```cpp
void KvServer::IfNeedToSendSnapShotCommand(int raftIndex, int proportion) {
  if (m_raftNode->GetRaftStateSize() > m_maxRaftState / 10.0) {   // proportion 没出现
```

调用点传的是 `9`（`kvServer.cpp:192`），意图显然是想表达"9/10"。但函数体写死了 `/10.0`。改了参数不会改变行为，是个陷阱。

### 4. `votedNum` / `appendNums` 的并发自增是数据竞争

```cpp
std::shared_ptr<int> votedNum = std::make_shared<int>(1);
// 每个 peer 一个 detached 线程，共享同一个 votedNum
*votedNum = *votedNum + 1;         // raft.cpp:800
if (*votedNum >= m_peers.size() / 2 + 1) { ... }
```

读-改-写不是原子的，`shared_ptr` 只保证指针本身的引用计数线程安全，**不保证它指向的 `int`**。多线程同时自增会丢失计数 → **可能选不出 leader**（票数永远到不了多数派）。虽然在 3 节点小集群里碰巧很少触发，但这是**未定义行为**。

`sendAppendEntries` 里的 `appendNums` 同理（`raft.cpp:882`），影响 commitIndex 推进。

正确做法：`std::atomic<int>`，或者更常见的——每个线程算自己的票，回来后由 leader 线程集中统计。

### 5. `leaderUpdateCommitIndex()` 是死代码

声明（`raft.h:92`）、实现（`raft.cpp:562`）都在，但**全项目没有任何调用点**——唯一一处是 `raft.cpp:903` 的注释 `// rf.leaderUpdateCommitIndex()`。实际生效的是 `sendAppendEntries` 里的内联版本，两者统计口径不同（见 [第 10 节](#10-commitindex-的推进)）。

### 6. `init` 中不恢复 `m_commitIndex`

```cpp
readPersist(m_persister->ReadRaftState());
if (m_lastSnapshotIncludeIndex > 0)
    m_lastApplied = m_lastSnapshotIncludeIndex;
    // rf.commitIndex = rf.lastSnapshotIncludeIndex   todo ：崩溃恢复为何不能读取commitIndex
```

且 `BoostPersistRaftNode` 压根没序列化 `m_commitIndex`。崩溃恢复后 `m_commitIndex` 从 0 开始，只能等新 leader 通过 AE 的 `leaderCommit` 把它推上去。注释里作者自己也标了 `todo`。

### 7. `m_lastRequestId` 的更新跨了两个线程

- `Get` 走 RPC 线程 → `ExecuteGetOpOnKVDB` 里更新（`kvServer.cpp:53`）
- `Put`/`Append` 走 apply 线程 → `ExecutePutOpOnKVDB` 里更新（`kvServer.cpp:70`）

两者操作的是**同一个 `m_lastRequestId`**，靠 `m_mtx` 保护。但 `Get` 的更新发生在 RPC 返回前，而 `Put` 的更新发生在 apply 时——**同一个客户端的 Get 和 Put 之间，去重表可能看到"跳号"**。Clerk 是串行发请求的（`m_requestId++` 后阻塞等待），所以实践中不触发，但这是巧合而非设计。

### 8. `Get` 走完整日志复制

`KvServer::Get` 调 `Raft::Start`，即**每次读都要写入一条日志、等多数派复制、再 apply**。这是最朴素但最昂贵的线性一致读实现。工业实现会用 ReadIndex 或 lease read 把读降为 1 次 RPC（Leader 确认自己仍是 leader + 本地状态机已追平），代价是推理更复杂。

### 9. `m_kvDB` 是未使用的残留成员

`kvServer.h:36` 声明了 `std::unordered_map<std::string, std::string> m_kvDB`，但**所有使用点都被注释掉了**（`kvServer.cpp:13, 28-31, 49-51, 69`）。同样在 `serialize()` 里也被注释（`kvServer.h:110`）。改为 SkipList 后没删干净。

### 10. 启动依赖固定 `sleep`

`kvServer.cpp:402` 的 `sleep(6)` 和 `:432` 的 `sleep(ipPortVt.size() - me)` 是纯时间同步。在慢机器或高负载下，这两个魔数不够会导致节点连接失败或 `Raft::init` 时序错乱。CLAUDE.md 里描述的"启动具有时序敏感性"正是指这个。

### 11. `Snapshot()` 里的日志拷贝低效

```cpp
// raft.cpp:1102-1110，注释自称 "这种写法有点笨，而且有内存泄漏的风险"
std::vector<raftRpcProctoc::LogEntry> trunckedLogs;
for (int i = index + 1; i <= getLastLogIndex(); i++)
    trunckedLogs.push_back(m_logs[getSlicesIndexFromLogIndex(i)]);
m_logs = trunckedLogs;
```

逐条 `push_back` 再整体赋值，可以直接用迭代器区间构造或 `erase`。数据量小时无感，日志大时是明显的无谓拷贝。

---

## 15. 与工业实现的差距

按"[当基础设施学](../raftClerk/raftClerk说明.md#1-模块定位)"的视角看，这个实现缺什么：

| 能力 | 本实现 | 工业系统（etcd / TiKV）为什么必须要 |
|---|---|---|
| **成员变更** | 完全没有。`raft.h` 里没有任何增删节点的接口，peer 列表在 `init` 时固定 | 集群扩容/缩容/换机器是日常运维。需要 joint consensus 或单步 ConfChange 保证变更过程中不出双 leader |
| **Pre-vote** | 没有 | 网络分区节点回归时 term 很高，会打断健康 leader 引发反复选举。Pre-vote 先"试探"再真正加 term |
| **ReadIndex / lease read** | 读走完整日志复制 | 读通常是写量的 10 倍以上，读也要复制一遍不可接受 |
| **增量日志 / WAL** | `persistData()` 每次序列化**全部** `m_logs` 并整体重写文件 | 日志百万条时每次 persist 都是秒级停顿。工业实现用追加式 WAL + 定期压缩 |
| **快照流式传输** | 整个快照作为一个 `bytes` 字段塞进一次 RPC | 快照可能几个 GB，必须分块传输、可断点续传 |
| **批量 / 流水线** | 一条命令一次 RPC，`Start` 后等心跳触发 | 吞吐量差一个数量级。需要 batching + pipelining（AppendEntries 不等响应就发下一批） |
| **日志冲突处理** | 逐条比对覆盖（`AppendEntries1`） | 论文标准的截断 + 回退优化，代码更简单。本实现为了容忍乱序而复杂化 |
| **异步 IO / 事件驱动** | 每节点每 peer 一个 detached `std::thread` | 千节点规模下线程数爆炸。工业实现用事件循环 / 协程 |

**这份清单本身就是"当基础设施学"的收获**：它把"能跑通"和"能上生产"之间的差距具体化了。每一条都可以作为下一步的深入方向。

---

## 附：建议的阅读路径

`raft.cpp` 是事件驱动的（三股控制流交织），**不要从第 1 行顺序读**。建议按下面的顺序切入：

1. **`Start`（raft.cpp:947）** —— 最短的入口，先建立"提案就是追加一条日志"的认识
2. **`applierTicker` + `getApplyLogs`（raft.cpp:155, 361）** —— 理解日志如何变成 `ApplyMsg`
3. **`KvServer::PutAppend` + `GetCommandFromRaft`（kvServer.cpp:212, 166）** —— 把上面两步接起来，看完整链路
4. **`doElection` + `sendRequestVote` + `RequestVote`（raft.cpp:202, 761, 611）** —— 选举
5. **`doHeartBeat` + `sendAppendEntries` + `AppendEntries1`（raft.cpp:247, 828, 8）** —— 日志复制，最后读 `AppendEntries1`
6. **`Snapshot` 三件套（raft.cpp:1087, 528, 420）** —— 快照
7. **`Persister`（Persister.cpp）** —— 落盘，最简单，适合收尾

配合实验：打开 `config.h` 里 `Debug = true`，在慢网络下把 `debugMul` 调大（比如 10），集群会频繁选举，`DPrintf` 的日志序列能把上面的流程"打印出来"。

---

## 相关文档

- [raftClerk 说明](../raftClerk/raftClerk说明.md) —— 客户端侧与线性一致性契约
- [目录导览](../../docs/目录导览.md)
- [CLAUDE.md](../../CLAUDE.md) —— 构建、运行与工作目录的坑
