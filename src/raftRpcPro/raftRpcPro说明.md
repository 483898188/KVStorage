# raftRpcPro 模块说明

> Raft 与 KV 服务的**接口定义层（IDL）**。这个模块里**没有一行手写逻辑**——
> 只有两个 `.proto` 文件，外加 7500 行提交进仓库的生成代码。
>
> 本文是 [raftClerk 说明](../raftClerk/raftClerk说明.md) 和 [raftCore 说明](../raftCore/raftCore说明.md) 的补充，
> 讲清楚"协议长什么样"以及"这些生成代码是怎么被 mprpc 用起来的"。

## 目录

- [1. 模块定位](#1-模块定位)
- [2. 文件清单](#2-文件清单)
- [3. 两个服务的分工](#3-两个服务的分工)
- [4. `raftRPC.proto` 逐字段](#4-raftrpcproto-逐字段)
- [5. `kvServerRPC.proto` 逐字段](#5-kvserverrpcproto-逐字段)
- [6. 与论文 Figure 2 的对照](#6-与论文-figure-2-的对照)
- [7. 生成代码里到底是什么](#7-生成代码里到底是什么)
- [8. 完整 RPC 调用链](#8-完整-rpc-调用链)
- [9. 关键约定](#9-关键约定)
- [10. 修改 proto 的正确流程](#10-修改-proto-的正确流程)
- [11. 已知疑点](#11-已知疑点)

---

## 1. 模块定位

这个模块的职责只有一件事：**定义节点间、以及客户端与服务器之间的数据格式**。

```
                     ┌─────────────────────────────────┐
                     │        raftRpcPro (本模块)       │
                     │   raftRPC.proto  ──┐            │
                     │  kvServerRPC.proto ┘ 仅定义消息  │
                     │        和 service 接口           │
                     └─────────────────────────────────┘
                              │ 生成
                              ▼
                   *.pb.h / *.pb.cc  （消息类 + Service/Stub）
                              │ 被使用
              ┌───────────────┴────────────────┐
              ▼                                ▼
      raftCore（Raft 继承 raftRpc，      mprpc（RpcProvider 按名字
       KvServer 继承 kvServerRpc）        分发到 CallMethod）
```

**关键认识**：生成了 `Service` / `Stub` 基类后，真正的网络传输**完全由 mprpc 负责**——protobuf 官方不提供 RPC 实现，只提供接口骨架。这个项目里"骨架"和"传输"是分开的两个模块：

| 关注点 | 归属 |
|---|---|
| 消息字段、方法签名 | `raftRpcPro`（本模块） |
| 序列化 / 反序列化 | protobuf 运行时 |
| 网络收发、服务注册、按名分发 | `src/rpc`（mprpc） |
| 方法的具体实现 | `src/raftCore` |

**这个模块的名字有拼写问题**：`raftRpcProctoc` 里的 `Proctoc` 疑似是 `Protocol` 的笔误，它同时是 proto 的 `package` 名、C++ 命名空间名，以及生成代码里所有符号的前缀。改名成本很高（要动所有 `.cpp`），所以一直留着。

---

## 2. 文件清单

| 文件 | 行数 | 手写？ | 说明 |
|---|---:|:---:|---|
| `raftRPC.proto` | 91 | ✅ | 节点间通信接口 |
| `kvServerRPC.proto` | 72 | ✅ | 面向客户端的接口 |
| `CMakeLists.txt` | 8 | ✅ | `aux_source_directory` 收集 `.pb.cc` |
| `raftRPC.pb.cc` | 2360 | ❌ | 生成 |
| `include/raftRPC.pb.h` | 2094 | ❌ | 生成 |
| `kvServerRPC.pb.cc` | 1341 | ❌ | 生成 |
| `include/kvServerRPC.pb.h` | 1724 | ❌ | 生成 |
| **合计** | **7690** | | **其中手写仅 171 行（2.2%）** |

**⚠️ 目录结构是手工调整过的**：`.proto` 和 `.pb.cc` 在模块根目录，但 `.pb.h` 放在 `include/` 下。protobuf 默认会把两个文件生成到同一目录，这里被人为分开了。**重新生成时要注意把 `.pb.h` 挪回 `include/`**，否则 include 路径会断。

**⚠️ 生成代码占了整个 `src/` 生成代码总量的绝大部分**：全仓库 `*.pb.*` 有 8337 行，这里就有 7519 行。读代码时应该直接跳过它们，只看两个 `.proto`。

---

## 3. 两个服务的分工

两个 proto 各定义一个 service，对应两种完全不同的通信模式：

| | `raftRPC.proto` | `kvServerRPC.proto` |
|---|---|---|
| **service 名** | `raftRpc` | `kvServerRpc` |
| **通信双方** | Raft 节点 ↔ Raft 节点 | Clerk → KvServer |
| **方向** | **双向**（A 给 B 发 AE，B 也会给 A 发 RequestVote） | **单向**（只有客户端发起） |
| **方法** | `AppendEntries`、`InstallSnapshot`、`RequestVote` | `Get`、`PutAppend` |
| **调用频率** | 高（心跳 25ms 一次 × 节点数） | 取决于客户端负载 |
| **实现者** | `Raft` 类（`raft.h:34`） | `KvServer` 类（`kvServer.h:25`） |

**这个区别解释了两处代码差异**（呼应 raftClerk 说明第 7 节）：

1. `src/raftCore/raftRpcUtil.h` 有 **caller + callee 两侧**；
   `src/raftClerk/raftServerRpcUtil.h` **只有 caller 一侧**。
2. `Raft` **公有继承** `raftRpc`；`KvServer` **私有继承** `kvServerRpc`（这是个不一致，见疑点）。

因为节点间是双向的，每个 Raft 节点**既要是客户端（主动发 RPC），又要是服务端（接收 RPC）**。而 KvServer 只被动接受请求，所以只需要 Stub 侧。

---

## 4. `raftRPC.proto` 逐字段

### 4.1 `LogEntry` — 日志条目

```protobuf
message LogEntry {
    bytes Command = 1;
    int32 LogTerm = 2;
    int32 LogIndex = 3;
}
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `Command` | `bytes` | 上层的命令。**注意是双层序列化**：`Op` 先被 boost text archive 序列化成字符串（`Op::asString`），再作为 `bytes` 装进这里 |
| `LogTerm` | `int32` | 该条日志产生时的任期 |
| `LogIndex` | `int32` | 逻辑 index（不是 `m_logs` 里的物理下标，见 raftCore 说明第 3 节） |

`(LogIndex, LogTerm)` 是 Raft 日志匹配的判据——`matchLog()` 就是比较这两个。

### 4.2 `AppendEntriesArgs` — 日志复制 / 心跳

```protobuf
message AppendEntriesArgs {
    int32 Term = 1;
    int32 LeaderId = 2;
    int32 PrevLogIndex = 3;
    int32 PrevLogTerm = 4;
    repeated LogEntry Entries = 5;
    int32 LeaderCommit = 6;
}
```

与论文 Figure 2 的 `AppendEntries RPC` **完全一致**。要点：

- **心跳就是 `Entries` 为空的 AE**——没有单独的心跳消息类型。
- `PrevLogIndex` / `PrevLogTerm` 是"前一条日志"的索引和任期，follower 用它做一致性检查。
- `LeaderCommit` 让 follower 知道可以推进到哪。

### 4.3 `AppendEntriesReply` — 本实现特有的两个字段

```protobuf
message AppendEntriesReply {
    int32 Term = 1;
    bool Success = 2;
    int32 UpdateNextIndex = 3;   // ← 论文没有
    int32 AppState = 4;          // ← 论文没有
}
```

论文只说 reply 要有 `Term` 和 `Success`。这里多了两个：

**`UpdateNextIndex` —— 回退加速**。论文的朴素做法是 follower 拒绝后 leader 把 `nextIndex--` 重试，一次只退一格，冲突日志很长时要往返很多轮。这里让 follower 直接算出"应该从哪个 index 重新开始"，一次跳过整个冲突 term（raftCore 说明第 9 节）。

**特殊值 `-100`**：表示"**不要回退**"。当 leader 的 term 比 follower 小（`args->term() < m_currentTerm`）时会设成 `-100`，因为这种情况 follower 根本没看日志，回退没有意义。这个魔法数字写死在 `raft.cpp:16` 和 `raft.cpp:872`。

**`AppState` —— 网络状态标识**。取值 `Disconnected = 0` / `AppNormal = 1`（`raft.h:23-25`）。配置文件里明确标注：

```
// todo：可以在rpc中删除该字段，实际生产中是用不到的.
```

它只用于调试时的网络分区观察（见疑点）。

### 4.4 `RequestVoteArgs` / `RequestVoteReply`

```protobuf
message RequestVoteArgs {
    int32 Term = 1;
    int32 CandidateId = 2;
    int32 LastLogIndex = 3;
    int32 LastLogTerm = 4;
}

message RequestVoteReply {
    int32 Term = 1;
    bool VoteGranted = 2;
    int32 VoteState = 3;    // ← 论文没有
}
```

`RequestVoteArgs` 与论文一致。`LastLogIndex`/`LastLogTerm` 是"选举限制"的实现基础——follower 靠它们判断候选人日志是否够新（raftCore 说明第 8 节的 `UpToDate`）。

**`VoteState` 是本实现独有的**，取值在 `raft.h:29-32`：

```cpp
constexpr int Killed = 0;
constexpr int Voted  = 1;   // 本轮已经投过票了
constexpr int Expire = 2;   // 投票（消息、竞选者）过期
constexpr int Normal = 3;
```

**但它其实没什么用**——`sendRequestVote`（`raft.cpp:796`）只看了 `reply->votegranted()`，从未读取 `votestate()`。服务端设置它只是为了调试日志。**这是一个定义了但没被消费的字段**。

### 4.5 `InstallSnapshotRequest` / `Response`

```protobuf
message InstallSnapshotRequest {
    int32 LeaderId = 1;
    int32 Term = 2;
    int32 LastSnapShotIncludeIndex = 3;
    int32 LastSnapShotIncludeTerm = 4;
    bytes Data = 5;      // 快照信息
}

message InstallSnapshotResponse {
    int32 Term = 1;      // 只需要 Term
}
```

对照论文 Figure 13，本实现**省略了 `Offset` 和 `Done` 字段**：

| 论文字段 | 本实现 | 后果 |
|---|---|---|
| `Data[]` | ✅ `bytes Data` | **整个快照塞进一次 RPC** |
| `Offset` | ❌ | 无法分块传输、无法断点续传 |
| `Done` | ❌ | 无法表达"还有后续分块" |

proto 文件里对 Response 的注释解释了为什么只返回 `Term`：

> 只用返回 Term，因为对于快照只要 Term 是符合的就是无条件接受的

这句话和 `CondInstallSnapshot` 恒返回 `true`（`raft.cpp:177`）是**同一个问题的两面**——快照的协商/拒绝机制在这个实现里被放弃了。

---

## 5. `kvServerRPC.proto` 逐字段

### `GetArgs` / `GetReply`

```protobuf
message GetArgs {
    bytes Key = 1;
    bytes ClientId = 2;
    int32 RequestId = 3;
}

message GetReply {
    bytes Err = 1;
    bytes Value = 2;
}
```

- `ClientId` + `RequestId` 是去重键 `(clientId, requestId)`——整个线性一致性契约的载体（raftClerk 说明第 8 节）。
- `Err` 是字符串，取值只有三个（`util.h:180-182`）：`"OK"`、`"ErrNoKey"`、`"ErrWrongLeader"`。**用 `bytes` 承载枚举**，没有用 proto 的 `enum`。

### `PutAppendArgs` / `PutAppendReply`

```protobuf
message PutAppendArgs {
    bytes Key = 1;
    bytes Value = 2;
    bytes Op = 3;          // "Put" or "Append"
    bytes ClientId = 4;
    int32 RequestId = 5;
}

message PutAppendReply {
    bytes Err = 1;
}
```

- `Op` 是**字符串** `"Put"` / `"Append"`，而不是 enum。服务端靠字符串比较来分派（`kvServer.cpp:182-187`）。
- **`Get` 和 `PutAppend` 是两个独立的 RPC**，不是 `Get(PutAppendArgs)` 带一个 op 字段。这与 MIT 6.824 的 `Get/PutAppend` 设计一致。

### 文件里残留的注释

`kvServerRPC.proto` 底部有一大段被注释掉的 `ResultCode` / `FriendServiceRpc` —— 那是 mprpc 框架自带的示例（好友服务），从别处抄来后没删干净。**对本项目没有任何作用**。

---

## 6. 与论文 Figure 2 的对照

把两个 proto 合起来看，能清楚看到"哪些状态要传输、哪些只在本地"：

| 论文中的状态 | 是否出现在 proto 里 | 为什么 |
|---|---|---|
| `currentTerm` | ✅ `Term` | 每个 RPC 都要带，收发双方都要比较 |
| `votedFor` | ❌ | **只在本地**：单机 Vote 决策依据，不需要传递 |
| `log[]` | ✅ `Entries` | 复制时传输 |
| `commitIndex` | ✅ `LeaderCommit` | leader 单向告知 follower |
| `lastApplied` | ❌ | 纯本地：状态机应用到哪了，别人不关心 |
| `nextIndex[]` | ❌ | **只在 leader 本地维护**，但通过 `UpdateNextIndex` 被 follower 间接影响 |
| `matchIndex[]` | ❌ | 纯 leader 本地统计 |
| `state` | ❌ | 由收到的 RPC 的 term 和内容推导，不用传 |
| `lastIncludedIndex/Term` | ✅ `LastSnapShotIncludeIndex/Term` | 快照 RPC 要带 |

**这张表本身就是个知识点**：Figure 2 列了 8 项状态，但只有 4 项会出现在网络上。`votedFor` / `nextIndex` / `matchIndex` / `lastApplied` 都是"本地的推理依据"，传输它们既无必要也无意义。理解这个划分，才算真正理解 Raft 的状态设计。

---

## 7. 生成代码里到底是什么

7519 行生成代码，实际只做三类事：

### 7.1 消息类（占大头）

每个 `message` 生成一个 C++ class，包含：

```cpp
class AppendEntriesArgs : public ::PROTOBUF_NAMESPACE_ID::Message {
  // getter / setter / mutable_ / clear_ / has_  … 每个字段一套
  int32_t term() const;
  void set_term(int32_t value);

  // 序列化接口
  bool SerializeToString(std::string*) const;
  bool ParseFromString(const std::string&);

  // 反射元数据（运行时描述自己的字段）
  static const ::PROTOBUF_NAMESPACE_ID::Descriptor* descriptor();
  ...
};
```

**命名映射规则**：proto 里写 `int32 Term = 1;`，生成的访问器是 `set_term()` / `term()`——**protobuf 会把字段名转成小写**。所以你会看到 `args->set_prevlogindex(...)` 而 proto 里写的是 `PrevLogIndex`。这解释了 raftCore 里那一堆小写访问器。

文件头部固定声明生成器的版本约束：

```cpp
#if PROTOBUF_VERSION < 3012000
#error This file was generated by a newer version of protoc ...
#endif
```

**这意味着用不同版本的 protoc 重新生成可能编译不过**。当前生成版本是 3.12.x（`3012004`）。

### 7.2 静态描述表

```cpp
// raftRPC.pb.cc
struct TableStruct_raftRPC_2eproto {
  static const ... ParseTableField entries[];
  static const ... schema[7];              // 7 个 message 的 schema
  ...
};
extern const ... DescriptorTable descriptor_table_raftRPC_2eproto;
static const ... ServiceDescriptor* file_level_service_descriptors_raftRPC_2eproto[1];
```

这些是**编译期生成的二进制元数据表**，运行时由 protobuf 的 `DescriptorPool` 加载。正是它们让"按字符串名字查找 service 和方法"成为可能——mprpc 的分发机制完全依赖于此（见下一节）。

### 7.3 Service 基类 + Stub（最关键的部分）

```cpp
// raftRPC.pb.h:1305
class raftRpc : public ::PROTOBUF_NAMESPACE_ID::Service {
 public:
  virtual ~raftRpc();
  typedef raftRpc_Stub Stub;
  static const ::PROTOBUF_NAMESPACE_ID::ServiceDescriptor* descriptor();

  // 三个虚方法 —— 服务端要重写这些
  virtual void AppendEntries(RpcController* controller,
                             const AppendEntriesArgs* request,
                             AppendEntriesReply* response,
                             Closure* done);
  virtual void InstallSnapshot(...);
  virtual void RequestVote(...);

  // implements Service
  void CallMethod(const MethodDescriptor* method, ...);   // ← 分发入口
};

// raftRPC.pb.h:1346
class raftRpc_Stub : public raftRpc {
 public:
  raftRpc_Stub(::PROTOBUF_NAMESPACE_ID::RpcChannel* channel);
  // 三个非虚方法 —— 客户端调用这些
  void AppendEntries(RpcController* controller, const AppendEntriesArgs* request,
                     AppendEntriesReply* response, Closure* done);
  ...
 private:
  ::PROTOBUF_NAMESPACE_ID::RpcChannel* channel_;
};
```

**基类的一行默认实现，是全模块最精髓的地方**（`raftRPC.pb.cc:2312`）：

```cpp
void raftRpc_Stub::AppendEntries(RpcController* controller,
                                 const AppendEntriesArgs* request,
                                 AppendEntriesReply* response,
                                 Closure* done) {
  channel_->CallMethod(descriptor()->method(0), controller, request, response, done);
}
```

Stub **不知道任何网络细节**——它只做一件事：拿到方法编号（这里是 `0`），交给 `channel_`。`channel_` 是本项目自己实现的 `MprpcChannel`。

---

## 8. 完整 RPC 调用链

以一次心跳 `AppendEntries` 为例，把"生成代码"和"mprpc"接起来。

### 8.1 客户端侧（发送）

```
Raft::doHeartBeat                    raft.cpp:305
  └─ std::thread(&Raft::sendAppendEntries, ...); t.detach();
       └─ Raft::sendAppendEntries    raft.cpp:836
            └─ m_peers[server]->AppendEntries(args.get(), reply.get())
                 └─ raftRpcProctoc::raftRpc_Stub::AppendEntries     【生成代码】
                      └─ channel_->CallMethod(descriptor()->method(0), ...)
                           └─ MprpcChannel::CallMethod    mprpcchannel.cpp:18
                                ├─ 需要的话 newConnect()
                                ├─ 从 method->service()->name() 取 "raftRpc"
                                ├─ 从 method->name() 取 "AppendEntries"
                                ├─ request->SerializeToString(&args_str)
                                ├─ 组 RPC::RpcHeader{service_name, method_name, args_size}
                                ├─ WriteVarint32(header 长度) + WriteString(header) + args_str
                                └─ send(m_clientFd, ...)
```

**注意 `descriptor()->method(0)` 这条路**：Stub 通过静态描述表拿到 `MethodDescriptor`，`MprpcChannel` 再从它身上反查出 service 名和方法名。**整个"客户端不需要手写 service 名"的能力，就来源于第 7.2 节那张静态表。**

### 8.2 服务端侧（接收）

```
RpcProvider::OnMessage                      rpcprovider.cpp:133   【muduo 回调】
  ├─ ReadVarint32(&header_size)
  ├─ PushLimit / ReadString / PopLimit  →  rpc_header_str
  ├─ RpcHeader.ParseFromString  →  service_name, method_name, args_size
  ├─ ReadString(&args_str, args_size)
  ├─ m_serviceMap.find("raftRpc")           ← NotifyService 时注册的
  ├─ m_methodMap.find("AppendEntries")
  ├─ request = service->GetRequestPrototype(method).New()     ← 用原型克隆出具体类型
  ├─ request->ParseFromString(args_str)
  ├─ response = service->GetResponsePrototype(method).New()
  ├─ done = NewCallback(&RpcProvider::SendRpcResponse, conn, response)
  └─ service->CallMethod(method, nullptr, request, response, done)
       └─ raftRpc::CallMethod                              【生成代码】
            └─ 按 method 编号分发 → 调用虚方法 AppendEntries
                 └─ Raft::AppendEntries            raft.cpp:926   【用户重写的】
                      ├─ AppendEntries1(request, response)   ← 真正的实现
                      └─ done->Run()                          → SendRpcResponse → conn->send()
```

`rpcprovider.cpp:224-232` 的中文注释把这段"为什么能调到业务代码"讲得很透彻，值得逐字读：

> 用户注册的 service 类继承 `.protoc` 生成的 serviceRpc 类，继承 `google::protobuf::Service`。
> 用户注册的 service 类没有重写 `CallMethod`，是生成的 serviceRpc 类重写了 `Service` 中的纯虚函数 `CallMethod`，
> 而生成的 serviceRpc 类会根据传入参数自动调用生成的 `xx` 方法（如 `Login`），
> 由于 `xx` 方法被用户注册的 service 类重写了，因此运行时会调用用户注册的 service 类的 `xx` 方法。**真的是妙呀**

**这就是"多态实现协议分发"的经典模式**：

```
Stub（客户端）──CallMethod──▶ Channel ──网络──▶ Provider ──CallMethod──▶ 虚函数 ──▶ 业务实现
   [生成]         [生成]      [mprpc]    [mprpc]  [生成]      [业务]
```

**服务注册**（`RpcProvider::NotifyService`，`rpcprovider.cpp:19`）不涉及网络，只是把 `ServiceDescriptor` 里的名字和方法表抄进 `m_serviceMap`：

```cpp
const ServiceDescriptor* pserviceDesc = service->GetDescriptor();
std::string service_name = pserviceDesc->name();          // "raftRpc"
for (int i = 0; i < pserviceDesc->method_count(); ++i)
    service_info.m_methodMap.insert({pserviceDesc->method(i)->name(), pserviceDesc->method(i)});
service_info.m_service = service;
m_serviceMap.insert({service_name, service_info});
```

`KvServer` 构造函数里注册了**两个** service（`kvServer.cpp:391-393`）：

```cpp
provider.NotifyService(this);                    // "kvServerRpc" → KvServer 自己
provider.NotifyService(this->m_raftNode.get());  // "raftRpc"   → Raft 对象
```

**同一个 `RpcProvider` 上同时发布客户端接口和节点间接口**——所以一个端口就够，不需要两个监听。

---

## 9. 关键约定

### 9.1 `cc_generic_services = true` —— 没有它就没有 Stub

两个 proto 都有一行：

```protobuf
option cc_generic_services = true;  //开启stub服务
```

**这行是必须的。** protobuf 官方从某个版本起**默认不生成 Service/Stub 代码**（因为官方不推荐用 generic services，认为该由 gRPC 之类的框架接管）。不加这个 option，只会生成消息类，不会有 `raftRpc` / `raftRpc_Stub`，mprpc 也就无从挂接。

**这是一个"用被官方标记为 deprecated 的特性来构建 RPC 框架"的设计**——能跑，但要清楚它的定位。

### 9.2 命名风格不统一

- proto 字段用 **CamelCase**（`PrevLogIndex`、`LeaderCommit`），而 protobuf 官方风格指南推荐 **snake_case**。
- service 和 method 也用 CamelCase（`AppendEntries`、`PutAppend`）。
- 生成后的 C++ 访问器是**全小写**（`set_prevlogindex()`）。

这不影响功能（protobuf 会做强转），但让"proto 里的字段名"和"代码里的访问器名"对不上号，读代码时需要一个转换动作。

### 9.3 `bytes` 与 `string` 的混用

proto3 里 `bytes` 和 `string` 在 C++ 中都映射到 `std::string`，所以功能上等价。但这个项目里：

- `Key` / `Value` / `Command` / `Err` 用 `bytes`；
- `Err` 承载的是 `"OK"` 这样的**文本**。

用 `bytes` 装文本、用 `bytes` 装枚举值，语义上都不够清晰，但能工作。

### 9.4 与 `rpcheader.proto` 的关系

`src/rpc/rpcheader.proto` 定义的是 **mprpc 框架自己的**协议头，不属于本模块：

```protobuf
package RPC;
message RpcHeader {
    bytes service_name = 1;
    bytes method_name = 2;
    uint32 args_size = 3;
}
```

**注意它用的是 `uint32`**，而业务 proto 里 index 之类的用 `int32`。类型选择不一致但无实际影响（都是变长编码）。

### 9.5 线上格式

```
[varint: header 长度][RpcHeader 序列化字节][业务请求序列化字节]
                     └─ service_name ─┘└─ args_size 指明长度 ─┘
```

请求有明确的**分帧**：先读 varint 长度，再按长度读 header，header 里有 `args_size`，再按长度读 args。**但响应侧没有分帧**，见疑点 1。

---

## 10. 修改 proto 的正确流程

**CMake 里没有任何 protoc 调用**——生成的 `.pb.cc` 是提交进仓库的，`aux_source_directory` 只是把它们收编。所以改了 proto 必须**手动重新生成**：

```bash
# 1. 生成（在模块根目录执行）
cd src/raftRpcPro
protoc raftRPC.proto --cpp_out=.
protoc kvServerRPC.proto --cpp_out=.

# 2. 把生成的 .pb.h 挪到 include/ 下（目录结构是手工调整过的）
mv raftRPC.pb.h include/
mv kvServerRPC.pb.h include/

# 3. 重新执行 cmake 配置（不是只重新 build！）
cd ../..
cmake -S . -B cmake-build-debug
cmake --build cmake-build-debug -j
```

**⚠️ 第 3 步不能省。** 因为源文件是用 `aux_source_directory` 收集的，它只在 **cmake 配置阶段**扫描目录。新增/删除/改名的源文件必须重新 configure，只 `--build` 不会发现。

**⚠️ protoc 版本敏感。** 生成代码里写死了版本检查（`PROTOBUF_VERSION < 3012000` 就 `#error`）。用和当前生成的 protobuf 运行库不兼容的 protoc 重新生成，会直接编译失败。当前是 **3.12.x**。

---

## 11. 已知疑点

> ⚠️ 前两条在 `src/rpc`（mprpc）里，但因为它们直接决定了本模块生成的 RPC 是否可用，所以放在这里。均为**静态阅读**发现，未运行验证。

### 1. 响应侧没有分帧，且缓冲区固定 1024 字节 —— 大响应必然失败

`mprpcchannel.cpp:103-124`：

```cpp
char recv_buf[1024] = {0};
int recv_size = 0;
if (-1 == (recv_size = recv(m_clientFd, recv_buf, 1024, 0))) { ... }
// 直接拿这一次 recv 的结果去解析
if (!response->ParseFromArray(recv_buf, recv_size)) { ... }
```

两个问题叠加：

1. **只 `recv()` 一次**。TCP 是字节流，一次 `recv` 不保证拿到完整消息——消息可能在多个 segment 里到达。
2. **缓冲区硬编码 1024**。超过 1024 字节的响应会被截断。

而请求侧是有完整分帧的（varint 长度 + header + args），**响应侧却没有**——这个不对称是设计缺陷。

**触发场景**：`GetReply` 里的 `Value` 超过约 1KB。此时 `ParseFromArray` 失败 → `controller->SetFailed()` → `raftServerRpcUtil::Get` 返回 `false` → Clerk 认为 RPC 失败，换一台服务器重试 → **仍然失败 → 无限轮转重试**（Clerk 的 `while(true)` 只对 `ErrWrongLeader`/失败做重试，见 raftClerk 说明疑点 1）。

`InstallSnapshotResponse` 和 `AppendEntriesReply` 都很小，所以 Raft 内部通信不受影响；**受影响的是存大 value 的 KV 场景**。

### 2. `MprpcChannel` 的 socket 在多线程间共享，无锁

```cpp
// MprpcChannel 始终复用同一个 m_clientFd（短连接注释与实现不符，实际是长连接）
int MprpcChannel::CallMethod(...) {
    if (m_clientFd == -1) newConnect(...);   // 无锁
    send(m_clientFd, ...);                    // 无锁
    recv(m_clientFd, recv_buf, 1024, 0);      // 无锁 ← 致命
}
```

`RaftRpcUtil` 的注释写得很清楚：*"对于一个 raft 节点来说，对于任意其他的节点都要维护一个 rpc 连接，即 MprpcChannel"*——**每个 peer 一个 channel**。

但 `Raft::doHeartBeat`（`raft.cpp:305`）**每轮心跳都给每个 peer 起一个新的 detached 线程**调 `sendAppendEntries`。如果某个 RPC 慢（还没返回），下一轮心跳（25ms 后）又会起线程用**同一个 `m_clientFd`** 发请求：

```
t=0ms   : 线程A → send(fd, AE#1) → 阻塞在 recv(fd)
t=25ms  : 线程B → send(fd, AE#2) → 阻塞在 recv(fd)   ← 同一个 fd！
```

两个线程抢同一个 fd 的 `recv`，**响应会随机错配给错误的请求**——A 可能收到 AE#2 的响应，B 可能收到 AE#1 的。在 Raft 里这会导致 `matchIndex` / `nextIndex` 计算错乱。

`leaderSendSnapShot` 与 `doHeartBeat` 之间也存在同样的竞争。

**这个 bug 与 Raft 层的 `votedNum` 数据竞争（raftCore 说明疑点 4）是同一类问题**：多线程 + 共享可变状态 + 无同步。区别是这个更难触发、更难复现。

### 3. `VoteState` 字段定义了但从未被读取

`RequestVoteReply.VoteState` 有 4 个取值（`Killed`/`Voted`/`Expire`/`Normal`），服务端在 `RequestVote` 里认真设置了它，但客户端 `sendRequestVote`（`raft.cpp:796`）只检查了 `reply->votegranted()`，**从未调用 `reply->votestate()`**。纯调试残留。

### 4. `AppState` 字段作者自认无用

`AppendEntriesReply.AppState` 只在两个地方被读：

- `sendAppendEntries` 里 `if (reply->appstate() == Disconnected) return;`（`raft.cpp:843`）
- `doHeartBeat` 里预置 `appendEntriesReply->set_appstate(Disconnected)`（`raft.cpp:303`）

而 `raftRPC.proto` 顶部的注释直接写着：

```
// todo：可以在rpc中删除该字段，实际生产中是用不到的.
```

它是**网络分区的调试开关**：预设为 `Disconnected`，`AppendEntries1` 一进来就覆盖成 `AppNormal`（`raft.cpp:10`）。如果 RPC 没送达，reply 保持 `Disconnected`，leader 就知道是网络问题而非日志不匹配。

### 5. `AppendEntriesReply.Term` 与 `AppState` 的语义重叠

`AppState == Disconnected` 时 `Term` 是未初始化的（默认 0）。但 `AppendEntries1` 的拒绝路径**都会设置 `Term`**，只有"RPC 根本没送到"时才是 0。这意味着：

```cpp
if (reply->term() > m_currentTerm)   // raft.cpp:850
```

在收到一个"没送到"的 reply（`term() == 0`）时不会误判——因为 0 不会大于 `m_currentTerm`。**这是靠"term 从 1 开始"这个隐含前提成立的**，代码里没有显式保证。如果哪天允许 `term == 0`，这里会出问题。

### 6. 快照无法分块传输

因为 proto 里缺 `Offset` / `Done`（第 4.5 节），`bytes Data` 必须装下**整个快照**。配合疑点 1 的 1024 字节响应缓冲，`InstallSnapshotRequest` 大起来之后的发送路径也需要检查（请求侧分帧是完整的，但 `muduo` 的 buffer 和内存占用仍是问题）。工业实现必须分块。

### 7. `KvServer` 私有继承

```cpp
class Raft    : public raftRpcProctoc::raftRpc          // raft.h:34        public
class KvServer : raftKVRpcProctoc::kvServerRpc          // kvServer.h:25    private（默认）
```

`provider.NotifyService(this)` 的形参是 `google::protobuf::Service*`，所以需要一次向上转型：

```
KvServer* ──(私有继承，仅类内可见)──▶ kvServerRpc* ──(公有继承)──▶ Service*
```

私有继承下第一步转换**只在 `KvServer` 的成员函数内合法**。调用点恰好就在 `KvServer` 的构造函数里（`kvServer.cpp:391`），所以能编译通过。**如果哪天要在类外把 `KvServer*` 交给 `NotifyService`，就会编译失败**——比如想改成"先构造对象、再单独注册"的写法时。

两个 service 对象用了不同的继承方式，是有意还是疏忽无法从代码判断，但**风格不统一本身就是维护负担**。

### 8. 重复的字段定义没有抽象

`GetArgs` 和 `PutAppendArgs` 都含 `Key` / `ClientId` / `RequestId`；`GetReply` 和 `PutAppendReply` 都有 `Err`；`RankArgs`/`Reply` 都有 `Term`。proto3 支持 `import` 和嵌套 message，但这里全部平铺重写。

在协议演化的场景下，这会带来兼容性风险——改一处去重字段的定义可能要同步改多处，漏一处就产生不一致。

---

## 附：这个模块值得学什么

说实话，**171 行 proto 定义 + 7519 行生成代码，可读的内容不多**。但有三点值得带走：

1. **IDL 与实现分离的工程结构**。协议定义、序列化、传输、业务实现分属四个模块，各自独立演进。改字段不会碰网络代码，换传输层不用改 proto。
2. **状态该不该上网，是设计问题不是实现问题**。第 6 节那张表——Figure 2 八项状态只有四项出现在网络上——这个划分是 Raft 设计的一部分，不是巧合。
3. **"生成代码 + 框架 + 业务"的三段式多态分发**。Stub 只转发 `descriptor()->method(i)`，框架只按字符串查表，业务只重写虚函数。三者互不知晓对方细节。这是所有 RPC 框架（包括 gRPC）的共同骨架。

**读法建议**：跳过所有 `.pb.*` 文件，只读两个 `.proto`（163 行），然后去 `src/rpc/rpcprovider.cpp:133-235` 和 `src/rpc/mprpcchannel.cpp:18-125` 看这两个端点如何被驱动。**协议本身很简单，价值在它被消费的方式上。**

---

## 相关文档

- [raftCore 说明](../raftCore/raftCore说明.md) —— 这些接口的服务端实现
- [raftClerk 说明](../raftClerk/raftClerk说明.md) —— `Get` / `PutAppend` 的客户端调用方
- [rpc 示例](../../example/rpcExample/rpc_example.md) —— mprpc 框架的独立用法
- [CLAUDE.md](../../CLAUDE.md) —— 构建、运行与工作目录的坑
