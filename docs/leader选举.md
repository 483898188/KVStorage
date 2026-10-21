# Leader 选举：一个随机定时器 + 三个语义重置点

> 项目：自研 Raft 共识 + 分布式 KV（`KVstorageBaseRaft-cpp`）
> 相关代码：`src/raftCore/raft.cpp`、`src/raftCore/include/raft.h`、`src/common/util.cpp`、`src/common/include/config.h`
> 相关文档：[日志匹配](./日志匹配.md)、[链路](./链路.md)、[持久化](./持久化.md)、[面试 Bug 清单](./面试Bug清单.md)

---

## 0. 一句话版本

**标准 Raft 选举（论文 §5.2），没有任何简化**——三个状态、随机化超时、RPC 拉票、日志新旧检查一应俱全。
它的骨架是**一个随机定时器 + 三个语义重置点**，其余都是推导出来的。

---

## 一、驱动它的那个定时器

选举不是「到点就选」，而是一个**自适应睡眠**的循环（`raft.cpp` 的 `electionTimeOutTicker`，
跑在 `monsoon::IOManager` 的协程里）：

```cpp
while (true) {
  while (m_status == Leader) usleep(HeartBeatTimeout);   // ① leader 不参选

  {   // ② 算出"应该睡到什么时候"
    m_mtx.lock();
    wakeTime = now();
    suitableSleepTime = getRandomizedElectionTimeout() + m_lastResetElectionTime - wakeTime;
    m_mtx.unlock();
  }
  usleep(...);

  if (m_lastResetElectionTime - wakeTime > 0) continue;  // ③ 睡眠期间计时器被重置过 → 没超时
  doElection();
}
```

关键在第 ③ 步：它记下睡前时间 `wakeTime`，醒来后看 `m_lastResetElectionTime` 有没有被别人推到
`wakeTime` 之后。有就说明这段时间收到了心跳或投了票，不算超时，重新睡。

**这就是「心跳续命」的实现方式——心跳不修改超时时长，只推移起点。**

配套的是随机化（`src/common/util.cpp:18`）：

```cpp
static thread_local std::mt19937 rng{std::random_device{}()};
std::uniform_int_distribution<int> dist(minRandomizedElectionTime, maxRandomizedElectionTime);
```

`min/max` 是 300/500 ms，心跳是 25 ms（`config.h:11`）——差一个数量级，符合论文建议，
能有效避免「多个 follower 同时超时、同时参选、票被瓜分、下一轮又同时超时」的活锁。

---

## 二、重置点：三个语义点，外加三处工程点

`m_lastResetElectionTime = now()` 在 `raft.cpp` 里一共有 **6 个赋值点**。
其中前三个对应论文要求的三条语义规则，后三个是工程上的补充：

| 位置 | 条件 | 性质 |
|---|---|---|
| `doElection`（`raft.cpp:227`） | 成为 Candidate，开始选举 | ✅ 论文要求 |
| `AppendEntries1`（`raft.cpp:40`） | 收到合法 AE，且 `args.term >= m_currentTerm` | ✅ 论文要求 |
| `RequestVote`（`raft.cpp:681`） | **只在真的投出去那一刻** | ✅ 论文要求 |
| `Raft::init`（`raft.cpp:1018`） | 节点启动 / 重启完成 | 工程补充 |
| `InstallSnapshot`（`raft.cpp:445`） | 接受了一个合法快照 | 工程补充 |
| `leaderSendSnapShot`（`raft.cpp:561`） | 对方 term 更高、自己退位 | 工程补充 |

第三点值得单独夸一句：**拒绝投票时不重置计时器**——这是论文的明确要求，
否则一个不断被拒的 candidate 会无限压制其他节点的选举。

同时 `AppendEntries1` 里对过期 leader 的处理也照顾到了：

```cpp
if (args->term() < m_currentTerm) { ...; return; }   // 注释：注意从过期的领导人收到消息不要重设定时器
```

这一条排在重置语句之前。**很多人在这里写错。**

---

## 三、发起选举：`doElection`

持 `m_mtx` 全程，做四件事：

```cpp
m_status = Candidate;
m_currentTerm += 1;              // 新一轮 = 新任期
m_votedFor = m_me;               // 自己投自己
persist();                       // ★ 先落盘
std::shared_ptr<int> votedNum = std::make_shared<int>(1);   // 自己一票
m_lastResetElectionTime = now();

for (int i = 0; i < m_peers.size(); i++) {
    if (i == m_me) continue;
    getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm);
    RequestVoteArgs{term, candidateid=m_me, lastlogindex, lastlogterm}
    std::thread t(&Raft::sendRequestVote, this, i, args, reply, votedNum);
    t.detach();                  // 并发拉票
}
```

三个细节：

- **`persist()` 在发 RPC 之前**。`currentTerm` 和 `votedFor` 是 Raft 的两个「必须持久化」状态，
  先落盘再对外发言，否则崩溃重启后可能在同一任期投两次票。
- **`votedNum` 用 `shared_ptr<int>` 而不是 `atomic`**。因为计票发生在 `sendRequestVote` 里、
  且必然持有 `m_mtx`（见第五节），锁已经保证了串行，`shared_ptr` 只是为了跨线程共享同一个计数器而已。
- **`m_peers[me]` 是 `nullptr`**，所以循环里必须 `continue`——这也是为什么 `m_peers` 要保持
  「下标 = server id」的完整形状。

---

## 四、收票方：`RequestVote` 的三道闸门

顺序是刻意排的，**不能调换**。

### 闸门 1：任期

```cpp
if (args->term() < m_currentTerm) { votegranted=false; votestate=Expire; return; }  // 过期竞选者
if (args->term() > m_currentTerm) { m_status=Follower; m_currentTerm=args->term(); m_votedFor=-1; }
```

### 闸门 2：日志新旧（论文 §5.4.1 的「选举限制」，保证 leader 完备性）

```cpp
if (!UpToDate(args->lastlogindex(), args->lastlogterm())) { votegranted=false; return; }

bool Raft::UpToDate(int index, int term) {      // 比较"最后一条日志(term, index)"的字典序
  getLastLogIndexAndTerm(&lastIndex, &lastTerm);
  return term > lastTerm || (term == lastTerm && index >= lastIndex);
}
```

### 闸门 3：是否已投

```cpp
if (m_votedFor != -1 && m_votedFor != args->candidateid()) { votegranted=false; return; }
m_votedFor = args->candidateid();
m_lastResetElectionTime = now();
reply->set_votegranted(true);
```

### 一个非标准字段

`RequestVoteReply` 里有 `VoteState`，取值是 `Killed/Voted/Expire/Normal`（`raft.h:29-32`）。
论文里没有这个东西，**发送方根本不看它**（`sendRequestVote` 只检查 `votegranted()`），
纯粹是给 `DPrintf` 打日志用的。属于可以删掉的冗余设计。

### 两个实现细节

- 整个函数被 `DEFER { persist(); }` 包着——**连拒绝投票的路径也会落盘**，安全但有点浪费。
- `RequestVote` 是 `AppendEntries1` 的镜像结构：同样的 term 三分支、同样的
  `myAssert(args->term() == m_currentTerm)` 兜底。

---

## 五、计票与登基：`sendRequestVote` 的回复处理

```cpp
if (reply->term() > m_currentTerm) {         // 别人任期更高 → 立刻退位
    m_status = Follower; m_currentTerm = reply->term(); m_votedFor = -1; persist(); return;
}
if (reply->term() < m_currentTerm) return;   // 旧回复，丢掉
if (!reply->votegranted()) return;

(*votedNum)++;
if (*votedNum >= m_peers.size() / 2 + 1) {   // 3 节点 → 需要 2 票
    *votedNum = 0;
    m_status = Leader;
    for (auto& n : m_nextIndex) n = lastLogIndex + 1;   // 有效下标从 1 起
    for (auto& m : m_matchIndex) m = 0;
    std::thread t(&Raft::doHeartBeat, this); t.detach(); // ★ 立即广播心跳，压制其他选举
    persist();
}
```

**收到回复必定先查 term**，这是 Raft 的通用铁律（`raft.cpp` 里有一处注释反复强调
「无论什么时候收到 rpc 请求和响应都要检查 term」）。

成为 leader 后立刻 `doHeartBeat()` 是关键——让所有 follower 的计时器归零，尽快结束动荡期。

---

## 六、持久化：重启后不重复投票

`persist()` / `readPersist()` 通过 `BoostPersistRaftNode` 序列化 5 个字段：
`currentTerm`、`votedFor`、`lastSnapshotIncludeIndex/Term`、`logs`（`raft.h:150-157`）。
前两个正是选举安全所需。

配合 `Raft::init` 的顺序——**先 `readPersist`，再设 `m_lastResetElectionTime = now()`**
（`raft.cpp:1018`）——节点重启后会带着原任期和原投票进入新一轮，
不会在同一个 term 里投给第二个人。这是「每个任期最多一个 leader」的底座。

（详见 [持久化](./持久化.md)。）

---

## 七、和论文比，缺了什么

| 论文特性 | 本项目 |
|---|---|
| 选举限制（日志新旧） | ✅ `UpToDate` |
| 随机化超时 | ✅ 300~500 ms |
| 任期持久化 | ✅ boost 持久化 |
| RequestVote 的「提前退位」逻辑 | ✅ 用 `DEFER persist()` + 三分支 |
| **PreVote** | ❌ 没有。分区节点高任期回归会扰动集群 |
| **Leader 租约 / ReadIndex** | ❌ 没有。读也走完整共识（安全但慢） |
| **领导权转移** | ❌ 没有 |
| **成员变更** | ❌ 没有，`m_peers` 启动时固定 |
| **AppendEntries 重试直到成功** | ❌ 只发一次；失联节点靠下一轮心跳补 |

后三/四项是「学习/参考实现」的合理取舍，但 **PreVote 的缺失**在故障恢复场景下会有实际影响：
一个被分区又回归的旧节点，任期已经被自己抬高，回来就会打断正常 leader，触发一轮无谓选举。

---

## 八、几个真实的隐患

### 1. `myAssert` 会 `exit` 整个进程

`src/common/util.cpp:5-10`。选举路径上有两处断言，都值得警惕：

```cpp
if (m_status == Leader) myAssert(false, "同一个term当两次领导，error");   // sendRequestVote
myAssert(reply->term() == m_currentTerm, ...);
```

**关于第一处，需要更正一个常见误判：它不可达，属于防逻辑错误的死代码。**

设节点数 `n`（`m_peers.size()` 是节点总数，含自己那槽）。当选需 `n/2 + 1` 票，
其中自己 1 票，故需外部赞成票 `n/2` 张。当选后仍在飞的拉票线程数为 `(n-1) - n/2`。
要再次触发断言，需这些线程**全部**返回赞成：

```text
(n-1) - n/2  >=  n/2 + 1
=>  n - 1     >=  n + 1        // 恒假
```

以 4 节点为例：阈值是 `4/2 + 1 = 3`，自己 1 票 + 2 张外部票当选，只剩 1 个线程在飞，
最多凑到 1 < 3。因此它只能在**逻辑写错**时触发，不能当作可达 bug。

（详细推导见 [面试 Bug 清单 §0.5](./面试Bug清单.md)。）

### 2. 客户端超时和选举超时贴得太近

`CONSENSUS_TIMEOUT = 500 ms`（`config.h:17`），而选举随机超时上限也是 500 ms。
一旦真的发生选举，客户端那 500 ms 的 `timeOutPop` 很可能在选举完成前就超时，
于是回 `ErrWrongLeader` 让 clerk 换节点——**功能上正确**（幂等保证安全），
但会放大延迟和重试量。把 `CONSENSUS_TIMEOUT` 调到 1~2 s 会稳得多。

### 3. 已经修好的两处

值得一提，因为它们暴露了原始实现的坑：

- `getRandomizedElectionTimeout` 原来每次调用都新建 `random_device`（Linux 上要读 `/dev/urandom`），
  而它是选举循环的**每轮必调**函数。现在改成 `static thread_local`（`util.cpp:18-19` 的注释记录了这段）。
- `now()` 用 `std::chrono::steady_clock` 而不是系统时钟（`util.cpp:14` 注释），
  否则 NTP 校时会直接破坏选举/心跳定时。

---

## 总结

选举逻辑本身是**忠实实现**：两个计时器（选举 300~500 ms 随机、心跳 25 ms）的搭配、
三个语义重置点的处理、以及 `persist()` 的时机都写对了。

真正的短板不在选举算法，而在周边——**没有 PreVote、断言会杀进程、客户端超时参数偏紧**。
