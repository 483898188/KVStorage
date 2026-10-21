# benchmark —— Raft KV 压测

面向 **4 核 8G** 预算设计的压测工具。包含集群启动、压测、读回校验、故障注入的完整流程。

```
benchmark/
├── bench.cpp       压测客户端（多线程 Clerk + 延迟分位 + 读回校验）
├── run_bench.sh    一键脚本：起集群 → 压测 → 收集群（支持故障注入）
├── CMakeLists.txt
└── README.md       本文件
```

---

## 1. 构建

```bash
mkdir -p cmake-build-debug && cmake -S . -B cmake-build-debug && cmake --build cmake-build-debug -j
```

产物在 `bin/`：`raftCoreRun`（集群启动器）、`bench`（压测客户端）。

> ⚠️ 新增了 `benchmark/` 目录后**必须重新执行 cmake 配置**，只 `--build` 不会发现它——因为项目用 `aux_source_directory`，只在配置阶段扫描目录（见 CLAUDE.md）。

---

## 2. 压测前**必须**改的两处

不改这两处，你测的是日志系统和快照机制，**不是 Raft**。

### ① `src/common/include/config.h`：`Debug` → `false`

```cpp
const bool Debug = true;      // ← 改成 false
```

**为什么**：`KvServer::GetCommandFromRaft` 每 apply 一条命令就调用 `DprintfKVDB()` → `m_skipList.display_list()`，**遍历所有层打印整个跳表**。这是 O(n) 输出 / 命令，累计 **O(n²)**，会完全淹没 Raft 的开销。

### ② `example/raftCoreExample/raftKvDB.cpp:58`：`maxraftstate` → 见下

```cpp
auto kvServer = new KvServer(i, 500, configFileName, port);
//                           ↑ 这个 500
```

**为什么**：快照阈值是 `m_maxRaftState / 10.0`（`IfNeedToSendSnapShotCommand`），`500/10 = 50` **字节**。而 `persistData()` 光是 term/votedFor/快照元信息就超过 50 字节——**几乎每 apply 一条命令就触发一次全量快照**（转储整个跳表 + 两层 boost 序列化 + 文件重写）。

**改成多少？** 这里有个反直觉的结论，见第 6 节。

**改完记得重新构建。**

---

## 3. 运行

```bash
# 默认：3 节点 / 2 客户端线程 / 每线程 2000 次写 / 绑核 0-3
./benchmark/run_bench.sh

# 覆盖参数
THREADS=4 OPS=5000 ./benchmark/run_bench.sh
MODE=get       ./benchmark/run_bench.sh      # 纯读（注意：Get 也走 Raft）
MODE=mix       ./benchmark/run_bench.sh      # 4 写 1 读
CPUSET=""      ./benchmark/run_bench.sh      # 不绑核

# 故障注入：压测开始 10s 后杀掉 node1
FAULT=kill FAULT_NODE=1 ./benchmark/run_bench.sh
FAULT=stop FAULT_NODE=1 ./benchmark/run_bench.sh   # 暂停（模拟分区/慢节点）
```

| 环境变量 | 默认 | 说明 |
|---|---|---|
| `NODES` | 3 | 集群节点数 |
| `THREADS` | 2 | 客户端线程数 |
| `OPS` | 2000 | 每线程操作数 |
| `MODE` | put | `put` / `get` / `mix` |
| `CPUSET` | `0-3` | `taskset` 绑核范围，空串则不绑 |
| `FAULT` | none | `none` / `stop` / `cont` / `kill` |
| `FAULT_DELAY` | 10 | 压测开始后多少秒注入 |
| `FAULT_NODE` | 1 | 对哪个节点下手（0-based） |

脚本会自动清理：起集群前 `rm -f bin/test.conf`（避免历史端口残留，见 `src/rpc/rpc说明.md` 疑点 4），退出时 `pkill -9 raftCoreRun`。集群日志在 `/tmp/raft_bench_cluster.log`。

**也可以手动跑**（更灵活，便于边压边观察）：

```bash
cd bin && rm -f test.conf && ./raftCoreRun -n 3 -f test.conf   # 终端 A
cd bin && ./bench -t 2 -n 2000 -m put                          # 终端 B
```

---

## 4. 4 核 8G 的资源预算

**瓶颈是 CPU，不是内存。**

**服务端**（每个 `KvServer` 进程的常驻线程）：

| 来源 | 线程数 | 依据 |
|---|---:|---|
| muduo TcpServer | 5 | 1 个事件循环 + `setThreadNum(4)`（`rpcprovider.cpp:91`） |
| IOManager 工作线程 | 1 | `FIBER_THREAD_NUM = 1` |
| `applierTicker` | 1 | `std::thread`（`raft.cpp:1036`） |
| 进程主线程 | 1 | 阻塞在 `t2.join()` |
| **小计** | **8** | |

**3 节点 × 8 ≈ 24 个常驻线程跑在 4 个核上 —— 已经 6 倍超配。**

**外加瞬时线程**：`doHeartBeat` 每 25ms 给每个 peer 起一个 detached `std::thread`。3 节点时 leader 每秒创建约 **80 个线程**（`raft.cpp:305`）。这才是 4 核上最痛的开销——线程创建要走 `clone()` 系统调用。

**内存**：完全不是问题。协程栈 128KB × 2 个 × 3 节点 ≈ 1MB；跳表和数据都在几十 MB 量级。8G 绰绰有余。

**所以客户端线程数给 2 就够**——服务端已经饱和，再加客户端线程只是增加上下文切换。建议测 `THREADS=1/2/4` 看曲线在哪变平。

**如果还想再榨一点**：把 `rpcprovider.cpp:91` 的 `setThreadNum(4)` 改成 `1-2`。在 4 核跑 3 节点时，12 个 muduo 工作线程纯属互相抢 CPU。这是源码改动，改完要重新构建。

---

## 5. 结果怎么读

```
[结果] 完成 4000 次操作，耗时 12.34s，吞吐 324 ops/s
[延迟] n=4000      avg=   6.12ms  P50=   5.80ms  P90=   9.40ms  P99=  28.10ms  max= 312.00ms
[校验] 检查 4000 个 key：丢失 0，值错误 0
[校验] ✅ 通过 —— 所有已确认的写入均可见且值正确
```

**三个数字比吞吐更重要**：

1. **P99 / max**。Raft 的 `Put` 要等多数派复制 + apply，延迟天然长尾。`max` 里如果有几百 ms 的尖峰，通常是发生了选举（选举超时 300~500ms）。

2. **校验结果**。`丢失 > 0` 意味着**已确认的写入丢了**——这是 Raft 提交安全性被破坏，比任何性能数字都严重。`值错误 > 0` 且返回值恰好等于 key，那是 `skipList::load_file` 的已知缺陷（[skipList说明 疑点 1](../src/skipList/skipList说明.md)）。

3. **QPS 随 OPS 的变化**。见第 6 节——**这个项目没有线性扩展的配置**，所以"QPS 随时间下降"本身就是一个结论。

**`Clerk::Put` 返回 `void`，失败对调用方不可见**（内部无限重试 `ErrWrongLeader`）。所以上面的"完成 4000 次"是**完成速率**，不是成功率——无法区分"很快"和"重试很多次才成功"。想统计真实成功率，得先给 `Clerk` 加返回值或计数器。

---

## 5.5 实测结果（3 节点 / 绑核 0-3 / 本机）

**配置未做任何优化**（`Debug=true`、`maxraftstate=500`），单客户端线程：

| 线程数 | 操作数 | 吞吐 | P50 | P99 |
|---:|---:|---:|---:|---:|
| 1 | 50 | 41 ops/s | 20.63ms | 31.44ms |
| 1 | 300 | 41 ops/s | 20.46ms | 31.60ms |
| 1 | 500 | 41 ops/s | 20.52ms | 31.58ms |
| 2 | 600 | 83 ops/s | 20.57ms | 31.67ms |
| 4 | 1200 | 166 ops/s | 21.09ms | 34.61ms |

**两个结论：**

**① 延迟被心跳周期锁死。** P50 稳定在 **20.5ms**，且**从 50 次到 500 次操作完全不变**——
说明在 n≤500 时 O(n) 的开销还没浮现，延迟几乎全部来自「等下一次心跳」。

原因在 `raft.cpp:969-971`：

> `// leader应该不停的向各个Follower发送AE来维护心跳和保持日志同步，目前的做法是新的命令来了不会直接执行，而是等待leader的心跳触发`

`Raft::Start` 追加日志后**不主动触发复制**，要等 `leaderHearBeatTicker`（25ms 周期）。
单线程吞吐 ≈ `1 / (心跳/2 + 复制 + apply)` ≈ 41 ops/s。

**② 吞吐随客户端并发线性增长，延迟不变。** 1→2→4 线程：41→83→166 ops/s，
**而 P50 几乎不动**。这正是"多个命令被批进同一次 AppendEntries"的效果——
心跳的固定成本被摊薄了。这也是本项目唯一"看起来合理"的伸缩行为。

> 上表未覆盖 `Debug=false` / 调优 `maxraftstate` 后的数字，留给你自己跑（第 2 节）。

---

## 6. 一个重要的理论限制：没有 O(N) 的配置

**改 `maxraftstate` 只能切换你在承受哪一种二次方成本**：

| 成本项 | 每次的代价 | 频率 |
|---|---|---|
| `persist()` | O(当前日志总长) | **每条命令**（`Raft::Start` / `AppendEntries1` 里都调） |
| `MakeSnapShot()` | O(当前跳表大小) | 每 `maxraftstate/10` 字节的日志 |

`persistData()` 每次都把**全部** `m_logs` 重新序列化并整体重写文件（`Persister::clearRaftState` 先 truncate）——**没有增量 WAL**。所以：

- `maxraftstate` **太大**（如 20MB）→ 快照很稀，但日志一直涨 → `persist` 每条的代价线性增长 → **O(N²)**；
- `maxraftstate` **太小**（如 500）→ `persist` 代价小，但快照极频繁，每次都要转储整个跳表 → 快照总数 × 递增的数据集大小 → **也是 O(N²)**。

**中间某个值能最小化两者的和。**

> ⚠️ 更正：我在之前的讨论里建议过把 `maxraftstate` 调到 20MB。**那个建议是错的**——它把频繁快照换成了每命令重写整个日志，在长跑里更糟。

**实用建议**：

- **短跑测 Raft 本身**（几百到几千次操作）：`maxraftstate = -1`（`GetCommandFromRaft` 里的 `if (m_maxRaftState != -1)` 会**完全关闭快照**），这样隔离出「Raft + persist」的成本。日志还小，`persist` 代价可接受。
- **长跑看退化**：`maxraftstate = 5000`（阈值 500 字节，日志频繁被裁剪）。然后**故意分别在 1k / 2k / 5k / 10k 次操作下各跑一遍**，观察 QPS 的下降曲线。

**这条退化曲线不是压测工具的缺陷，而是这个实现的设计属性**——工业实现（etcd / TiKV）用追加式 WAL + 定期压缩来避免它。看懂这条曲线，比拿到一个漂亮的 QPS 数字有价值得多。

---

## 7. 故障注入

脚本会在压测进行中注入故障。**推荐的测试剧本**（按顺序，每步都做读回校验）：

| # | 命令 | 模拟 | 预期 |
|---|---|---|---|
| 1 | `./run_bench.sh` | 无故障 | 基线 QPS / P99 |
| 2 | `FAULT=stop FAULT_NODE=1 ./run_bench.sh` | follower 卡死（分区） | P99 抬升，QPS 略降，**不丢数据** |
| 3 | `FAULT=kill FAULT_NODE=1 ./run_bench.sh` | follower 崩溃 | 影响很小（3 节点容 1 个） |
| 4 | `FAULT=kill FAULT_NODE=0 ./run_bench.sh` | 若 node0 恰是 leader → 重新选举 | **一段明显的延迟尖峰**（300~500ms）+ QPS 掉落再恢复 |

**第 4 项最有价值**——它一次性验证选举、日志复制、客户端重试三条链路。

**手动注入**（在另一个终端）：

```bash
pgrep -f raftCoreRun            # 找到各节点 pid
kill -STOP <pid>                # 暂停 → 模拟分区/慢节点
kill -CONT <pid>                # 恢复
kill -9   <pid>                 # 崩溃
```

**我预计第 2、4 项会暴露出问题**（静态阅读的推断，未验证）：

- `CONSENSUS_TIMEOUT = 500ms` 恰好等于最大选举超时 → 换届必然产生一批客户端超时重试；
- `votedNum` / `appendNums` 是 `shared_ptr<int>` **无锁自增**（[raftCore说明 疑点 4](../src/raftCore/raftCore说明.md)）→ 高并发下可能丢票 → **选举卡住**；
- `MprpcChannel` 的 socket 在多线程间共享、无锁、无超时（[rpc说明 疑点 2](../src/rpc/rpc说明.md)）→ 慢 RPC 与下一轮心跳抢同一个 fd 的 `recv`。

**如果第 4 项出现「选举超过 5 秒未恢复」或「集群再也没有 leader」，基本可以确认是 `votedNum` 那个数据竞争。**

---

## 8. 已知会撞到的墙

按撞上的先后顺序：

| # | 现象 | 原因 | 处理 |
|---|---|---|---|
| 1 | 压测一跑就卡死，QPS 极低 | `Debug=true` → 每命令 O(n) 打印跳表 | 改 `Debug=false` |
| 2 | 几百次操作后急剧变慢 | 阈值 50 字节 → 每命令一次全量快照 | 改 `maxraftstate` |
| 3 | 写大 value 后请求永久失败 | mprpc 响应无分帧 + 1024 字节缓冲（[rpc说明 疑点 1](../src/rpc/rpc说明.md)） | 保持 value < 1KB |
| 4 | 集群没起来，程序永久挂住 | `Clerk` 无限重试 + mprpc 无超时 | 看 `bench` 的看门狗输出；检查集群日志 |
| 5 | 高并发下偶发选举卡死 | `votedNum` 数据竞争 | 暂无修复，属观察目标 |
| 6 | 长跑 QPS 单调下降 | `persist()` 是 O(日志长度) | 见第 6 节，这是设计属性 |

**`bench` 有一个 5 秒心跳的看门狗线程**，会持续打印"已完成 N / M 次操作"。如果这个数字长时间不动，说明卡住了（多半是集群挂了），不是慢。

---

## 8.5 压测中发现并已修复的缺陷

用这套工具跑出来的问题，**都已修复并逐一验证**（每条的验证方式见末列）。

| # | 缺陷 | 后果 | 验证 |
|---|---|---|---|
| 1 | `Clerk::Uuid()` 用 `rand()` 但从不 `srand()`（`clerk.h`） | **跨进程 clientId 完全相同** → 第二个客户端进程的写入全被去重表判为重复而不执行，却回 `OK` → **静默丢写** | `dedup_test.sh`：修复前进程#2 `值错误 300`，修复后 `0` |
| 2 | `Persister` 构造函数用 `trunc` 打开落盘文件（`Persister.cpp`） | 每次启动都把上次状态抹掉 → **崩溃恢复从未生效**，Raft 的持久化要求被架空 | `persist_probe.sh`：修复前重启读到空串，修复后读到完整内容 |
| 3 | `Persister::ReadXxx` 用 `ifs >> str` | `operator>>` 遇空白即停，而 raft/snapshot 是 boost 文本归档，满是空格 → 只能读到第一个词 | 同 #2 |
| 4 | `Persister` 读完后用默认 mode 重开写流 | `ofstream::open` 默认是 `out`，**会截断** —— 读完就把文件清零 | 同 #2 |
| 5 | `Persister` 写入后不 `flush` | 数据停在用户态缓冲，进程被 `SIGKILL` 即丢失 → `persist()` 的"响应前置落盘"语义不成立 | 修复前后对比 `bin/raftstatePersist*.txt` 大小：0 字节 → 380 字节 |
| 6 | `skipList::load_file` 两个参数都传 `keyDumpVt_[i]` | 快照恢复后**每个 value 都变成 key 本身**（`Put("name","alice")` → `name→name`） | 代码审阅 + 读回校验可检出"值错误" |
| 7 | `skipList::load_file` 不清空现有数据 | 装快照变成"合并"而非"替换"；又因 `insert_element` 不更新已存在的 key，结果既非旧态也非新态 | 同上 |
| 8 | `AppendEntries1` 快照分支缺 `return`（`raft.cpp`） | 落到 `matchLog` 的断言 → `myAssert` 调 `std::exit()` → **节点进程直接退出** | 代码审阅（触发条件是 follower 快照领先 leader，属正常场景） |
| 9 | `MprpcChannel` 用 `send(fd, ..., 0)`（`mprpcchannel.cpp`） | 对端一挂，内核发 `SIGPIPE` → **本端进程被打死**，下面"重连重发"的逻辑根本执行不到 | `FAULT=kill` 实测：修复前 `exit=141`（128+SIGPIPE），修复后正常完成 |
| 10 | `now()` 用 `high_resolution_clock`（`util.cpp`） | libstdc++ 下它 == `system_clock`，**非单调**（实测 `is_steady==0`）→ 一次 NTP 校时就破坏选举/心跳定时 | 编译期 `static_assert`/类型检查 |
| 11 | `Op::RequestId` 未初始化（`util.h`） | 该成员直接参与去重判断，随机值会造成误判重复（丢写）或漏判（重复执行） | 代码审阅 |
| 12 | `LockQueue::timeOutPop` 用 `system_clock` | 同 #10，时钟回拨会让等待提前/延后返回 | — |
| 13 | `getRandomizedElectionTimeout` 每次重建 `random_device` | 每轮循环一次 `/dev/urandom` 系统调用（热路径） | — |
| 14 | `MprpcConfig` 用 `insert` | 重复 key 时**静默取陈旧值** → 表现为"节点连不上对端"且无任何报错 | 现在会打印覆盖告警 |

**其中 #1、#2–#5、#9 是实测复现过的**，其余为静态审阅。

修复后系统行为有一个**需要留意**的变化：**节点重启会真的恢复状态了**（此前永远从空白开始）。所以 `run_bench.sh` 默认会清空持久化文件（`CLEAN_STATE=0` 可保留，用于验证恢复路径）。

---

## 9. 和自带 `callerMain` 的区别

`example/raftCoreExample/caller.cpp` 不是压测程序：

```cpp
auto start = now();                                   // 拿了时间却没用
int count = 500;                                      // 样本太小
while (tmp--) {
  client.Put("x", std::to_string(tmp));
  std::string get1 = client.Get("x");
  std::printf("get return :{%s}\r\n", get1.c_str());  // 每条 printf —— 测的是 stdout
}                                                     // 返回值从不校验
```

它验证"能通"，不测性能。`bench` 相对它的改动：

- 去掉全部逐条输出；
- 用 `steady_clock` 真正计时，输出 P50/P90/P99/max；
- 多线程，**每线程独立 `Clerk`**（`Clerk` 无锁，共享会读到撕裂的值）；
- 线程私有 key 空间，避免并发写同 key 干扰校验；
- 写完后全量读回校验；
- 看门狗线程，区分"慢"和"挂死"。

---

## 相关文档

- [src/common/common说明.md](../src/common/common说明.md) —— `Debug` / `CONSENSUS_TIMEOUT` 等参数
- [src/raftCore/raftCore说明.md](../src/raftCore/raftCore说明.md) —— 疑点 4（`votedNum` 数据竞争）、第 12 节（持久化）
- [src/rpc/rpc说明.md](../src/rpc/rpc说明.md) —— 疑点 1（响应分帧）、疑点 2（socket 竞态）
- [src/skipList/skipList说明.md](../src/skipList/skipList说明.md) —— 疑点 1（`load_file` 把 key 写成 value）
- [CLAUDE.md](../CLAUDE.md) —— 构建、运行与工作目录的坑
