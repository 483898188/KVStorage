# rpc（mprpc）模块说明

> 自研的 protobuf RPC 框架，代号 **mprpc**。它是 Raft 的**传输层**，不含任何共识/存储逻辑。
>
> 本文是 [raftRpcPro 说明](../raftRpcPro/raftRpcPro说明.md) 的续篇。那份讲"协议长什么样"，
> 这份讲"这些协议怎么变成字节、怎么进出网络"。

## 目录

- [1. 模块定位](#1-模块定位)
- [2. 文件清单](#2-文件清单)
- [3. 五个类的关系](#3-五个类的关系)
- [4. 线上格式](#4-线上格式)
- [5. 客户端路径](#5-客户端路径)
- [6. 服务端路径](#6-服务端路径)
- [7. Controller：错误如何穿过框架](#7-controller错误如何穿过框架)
- [8. Config：配置文件解析](#8-config配置文件解析)
- [9. 一个 RPC 框架最少需要什么](#9-一个-rpc-框架最少需要什么)
- [10. 已知疑点](#10-已知疑点)
- [11. 与工业 RPC 框架的对比](#11-与工业-rpc-框架的对比)

---

## 1. 模块定位

protobuf 官方只生成 `Service` / `Stub` 骨架，**不提供任何网络实现**。mprpc 干的就是把骨架和 TCP 接起来这件事：

```
     protobuf 提供的抽象                 mprpc 提供的实现
  ────────────────────────      ──────────────────────────────
   google::protobuf::RpcChannel  →   MprpcChannel        （客户端发送）
   google::protobuf::RpcController →  MprpcController     （错误传递）
   google::protobuf::Service     →   RpcProvider         （服务端注册+分发）

   生成代码（raftRpcPro）          →   被上面三个类驱动
```

**这个模块与业务完全解耦**：它不知道 Raft 存在，不知道 `Op` 是什么。它只认识 `MethodDescriptor`、`Message`、`Service` 这些 protobuf 抽象类型。

代码里唯一提业务的地方是 `RpcProvider::Run` —— 它为把本机 ip/port 写进 `test.conf` 而硬编码了 `node<i>ip` / `node<i>port` 的键名（`rpcprovider.cpp:63-71`）。**这是框架层被业务需求污染的一处**，也是 CLAUDE.md 里那个"工作目录的坑"的根源。

---

## 2. 文件清单

| 文件 | 行数 | 手写？ | 说明 |
|---|---:|:---:|---|
| `mprpcchannel.cpp` / `.h` | 171 / 38 | ✅ | 客户端：序列化 + 发送 + 接收 |
| `rpcprovider.cpp` / `.h` | 254 / 44 | ✅ | 服务端：注册 + 分发 |
| `mprpcconfig.cpp` / `.h` | 70 / 18 | ✅ | `key=value` 配置文件解析 |
| `mprpccontroller.cpp` / `.h` | 24 / 20 | ✅ | 错误状态容器 |
| `rpcheader.proto` | 9 | ✅ | 自定义协议头 |
| `rpcheader.pb.cpp` | 357 | ❌ | 生成 |
| `include/rpcheader.pb.h` | 461 | ❌ | 生成 |
| `CMakeLists.txt` | 8 | ✅ | 另建 `rpc_lib` |
| **合计** | **1466** | | **手写 643 行，生成 818 行** |

**两个值得注意的细节**：

1. **`rpcheader.pb.cpp` 的扩展名是 `.cpp` 不是 `.cc`** —— 全项目其他生成文件都是 `.pb.cc`，只有它是 `.pb.cpp`。这个不一致有实际后果，见[疑点 3](#10-已知疑点)。
2. **CMakeLists 同时用了两套机制**：`add_library(rpc_lib ...)` 建了一个库，又 `set(src_rpc ...)` 导出变量供根 CMakeLists 聚合进 `skip_list_on_raft`。同一批源文件被两个目标收录。

---

## 3. 五个类的关系

```
                    ┌──────────────────────────────────────┐
        客户端       │  生成代码 raftRpc_Stub                 │
                    │    └─ channel_->CallMethod(method, …) │
                    └───────────────┬──────────────────────┘
                                    ▼
                    ┌──────────────────────────────────────┐
                    │  MprpcChannel  : RpcChannel           │
                    │    序列化 → 组 header → send → recv    │
                    └───────────────┬──────────────────────┘
                                    │  TCP
     ───────────────────────────────┼───────────────────────────
                                    ▼
                    ┌──────────────────────────────────────┐
        服务端       │  RpcProvider                          │
                    │    注册(NotifyService) / 分发(OnMessage)│
                    │    └─ service->CallMethod(method, …)   │
                    └───────────────┬──────────────────────┘
                                    ▼
                    ┌──────────────────────────────────────┐
                    │  生成代码 raftRpc（基类 CallMethod）    │
                    │    └─ 按编号分发到虚函数                │
                    └───────────────┬──────────────────────┘
                                    ▼
                    ┌──────────────────────────────────────┐
                    │  Raft::AppendEntries（业务重写）        │
                    └──────────────────────────────────────┘
```

| 类 | 基类/角色 | 职责 |
|---|---|---|
| `MprpcChannel` | 实现 `google::protobuf::RpcChannel` | 客户端：序列化、组包、收发 |
| `RpcProvider` | 无（组合 muduo `TcpServer`） | 服务端：注册 service、解析请求、分发、回响应 |
| `MprpcController` | 实现 `google::protobuf::RpcController` | 在框架和业务之间传递错误 |
| `MprpcConfig` | 无 | 解析 `key=value` 配置文件 |
| `RPC::RpcHeader` | 生成的消息类 | 自定义协议头 |

**两个"协议头"要分清**：

| | 定义位置 | 作用 |
|---|---|---|
| `RPC::RpcHeader` | `src/rpc/rpcheader.proto` | **mprpc 框架自己的**：`service_name` / `method_name` / `args_size` |
| 业务消息 | `src/raftRpcPro/*.proto` | `AppendEntriesArgs` 等，是 RpcHeader 后面挂的"载荷" |

---

## 4. 线上格式

请求的字节布局（`mprpcchannel.cpp:57-73` 构造）：

```
┌────────────┬──────────────────────┬──────────────────────┐
│ varint     │  RpcHeader 序列化字节  │  业务请求序列化字节     │
│ header长度  │                      │                      │
└────────────┴──────────────────────┴──────────────────────┘
              ▲                      ▲
              │ RpcHeader.args_size 指明这段的长度
```

`RpcHeader` 本身（`rpcheader.proto`）：

```protobuf
message RpcHeader {
    bytes service_name = 1;    // "raftRpc"
    bytes method_name = 2;     // "AppendEntries"
    uint32 args_size = 3;      // 后面那段业务字节的长度
}
```

**为什么用 varint 而不是固定 4 字节？** 注释里作者引了一篇博客解释：

> 这里虽然是 uint32，但是 protobuf 编码的时候默认就是变长编码

小的 header 长度（通常几十字节）用 1 个字节就能表示，省 3 字节。在心跳这种高频小包场景下有意义。

**请求侧的分帧是完整的**：客户端写 varint 长度 → header → args；服务端 `ReadVarint32` → `PushLimit/ReadString/PopLimit` → `ReadString(args_size)`。**响应侧没有分帧**，见[疑点 1](#10-已知疑点)。

---

## 5. 客户端路径

`MprpcChannel::CallMethod`（`mprpcchannel.cpp:18`）—— 所有 Stub 调用的终点：

```cpp
void MprpcChannel::CallMethod(const MethodDescriptor* method, RpcController* controller,
                              const Message* request, Message* response, Closure* done) {
  // ① 惰性连接
  if (m_clientFd == -1) {
    bool rt = newConnect(m_ip.c_str(), m_port, &errMsg);
    if (!rt) { controller->SetFailed(errMsg); return; }
  }

  // ② 从 MethodDescriptor 反查 service/method 名
  const ServiceDescriptor* sd = method->service();
  std::string service_name = sd->name();
  std::string method_name  = method->name();

  // ③ 序列化请求
  uint32_t args_size{};
  std::string args_str;
  if (!request->SerializeToString(&args_str)) { controller->SetFailed("serialize request error!"); return; }

  // ④ 组 RpcHeader
  RPC::RpcHeader rpcHeader;
  rpcHeader.set_service_name(service_name);
  rpcHeader.set_method_name(method_name);
  rpcHeader.set_args_size(args_size);
  std::string rpc_header_str;
  rpcHeader.SerializeToString(&rpc_header_str);

  // ⑤ 拼字节流：varint(header长度) + header + args
  std::string send_rpc_str;
  {
    google::protobuf::io::StringOutputStream string_output(&send_rpc_str);
    google::protobuf::io::CodedOutputStream coded_output(&string_output);
    coded_output.WriteVarint32(static_cast<uint32_t>(rpc_header_str.size()));
    coded_output.WriteString(rpc_header_str);
  }
  send_rpc_str += args_str;

  // ⑥ 发送（失败则重连后重发，无限重试）
  while (-1 == send(m_clientFd, send_rpc_str.c_str(), send_rpc_str.size(), 0)) {
    close(m_clientFd); m_clientFd = -1;
    if (!newConnect(m_ip.c_str(), m_port, &errMsg)) { controller->SetFailed(errMsg); return; }
  }

  // ⑦ 接收
  char recv_buf[1024] = {0};
  int recv_size = recv(m_clientFd, recv_buf, 1024, 0);
  if (-1 == recv_size) { close(m_clientFd); m_clientFd = -1; controller->SetFailed(errtxt); return; }

  // ⑧ 反序列化响应
  if (!response->ParseFromArray(recv_buf, recv_size)) { controller->SetFailed(errtxt); return; }
}
```

**关键点**：

- **② 的 `method->service()->name()`** 是"客户端不用手写 service 名"的来源——名字从生成代码的静态描述表里反查出来。
- **⑥ 的无限重试**：`send` 失败就重连再发，**没有次数上限**，也没有超时。
- **⑦ 的 `recv` 是阻塞的**，且 socket 没有设 `SO_RCVTIMEO`——**对端不响应就永久挂住**。
- **`newConnect` 的 `connectNow` 参数**：`RaftRpcUtil` 传 `true`（立即连接），`raftServerRpcUtil` 传 `false`（延迟到首次调用）。这和启动时序有关——Clerk 可能比服务器先起来。

**这个函数没有任何锁。** `m_clientFd` 是共享可变状态，见[疑点 2](#10-已知疑点)。

---

## 6. 服务端路径

### 6.1 `NotifyService` —— 纯注册，不碰网络（`rpcprovider.cpp:19`）

```cpp
void RpcProvider::NotifyService(google::protobuf::Service* service) {
  ServiceInfo service_info;
  const ServiceDescriptor* pserviceDesc = service->GetDescriptor();
  std::string service_name = pserviceDesc->name();
  int methodCnt = pserviceDesc->method_count();

  for (int i = 0; i < methodCnt; ++i) {
    const MethodDescriptor* pmethodDesc = pserviceDesc->method(i);
    service_info.m_methodMap.insert({pmethodDesc->name(), pmethodDesc});
  }
  service_info.m_service = service;
  m_serviceMap.insert({service_name, service_info});
}
```

只是把 `ServiceDescriptor` 里的名字抄进两个 map：

```cpp
struct ServiceInfo {
    google::protobuf::Service* m_service;
    std::unordered_map<std::string, const google::protobuf::MethodDescriptor*> m_methodMap;
};
std::unordered_map<std::string, ServiceInfo> m_serviceMap;
```

**这就是"字符串 → 方法"的路由表。** 它完全来自生成代码的静态描述表（raftRpcPro 说明第 7.2 节），不需要手写任何注册宏。

`KvServer` 上注册了两个 service（`kvServer.cpp:391-393`）：

```cpp
provider.NotifyService(this);                    // "kvServerRpc"
provider.NotifyService(this->m_raftNode.get());  // "raftRpc"
```

**一个端口同时提供客户端接口和节点间接口**——因为 `raftRpc` 和 `kvServerRpc` 名字不同，路由表不会冲突。

### 6.2 `Run` —— 启动 + 写配置文件（`rpcprovider.cpp:42`）

```cpp
// ① 取本机 ip
gethostname(hname, sizeof(hname));
hent = gethostbyname(hname);
for (int i = 0; hent->h_addr_list[i]; i++)
    ipC = inet_ntoa(*(struct in_addr*)(hent->h_addr_list[i]));   // 循环只为拿最后一个
std::string ip = std::string(ipC);

// ② 把 node<i>ip/port 追加写入当前目录的 test.conf   ← 框架被业务污染的地方
std::string node = "node" + std::to_string(nodeIndex);
std::ofstream outfile;
outfile.open("test.conf", std::ios::app);       // 追加
outfile << node + "ip=" + ip << std::endl;
outfile << node + "port=" + std::to_string(port) << std::endl;
outfile.close();

// ③ 建 muduo 服务器
muduo::net::InetAddress address(ip, port);
m_muduo_server = std::make_shared<muduo::net::TcpServer>(&m_eventLoop, address, "RpcProvider");
m_muduo_server->setConnectionCallback(std::bind(&RpcProvider::OnConnection, this, _1));
m_muduo_server->setMessageCallback(std::bind(&RpcProvider::OnMessage, this, _1, _2, _3));
m_muduo_server->setThreadNum(4);

// ④ 启动并进入事件循环（阻塞，永不返回）
m_muduo_server->start();
m_eventLoop.loop();
```

**② 是理解整个项目运行方式的关键，也是最大的坑**：

- 写的是**当前工作目录**的 `test.conf`，不是 `-f` 指定的路径；
- 用 `std::ios::app` **追加**，从不清理；
- 节点启动端口是在 `[10000, 29999]` 随机选的，每次运行都不同。

于是：**每跑一次集群，`test.conf` 就多出 2×节点数 行**。仓库里 committed 的 `bin/test.conf` 已经出现了重复：

```
node0ip=127.0.1.1
node0port=29016
node1ip=127.0.1.1
node1port=29017
node2ip=127.0.1.1
node2port=29018
node1ip=127.0.1.1      ← 重复！
node1port=7788         ← 与上面的 29017 冲突
```

一个 key 有两个值，读的时候用哪个？取决于 `MprpcConfig` —— 而它用的是 `insert`（**首个生效**），见[疑点 4](#10-已知疑点)。**这意味着 stderr 上的旧端口会遮蔽新端口**。

**① 的循环写法有隐患**：`for` 循环每次都覆盖 `ipC`，只保留最后一个地址。在有多张网卡的机器上，取到哪个是不确定的。注释掉了的 `getReleasePort` 说明作者也想过端口占用问题（`rpcprovider.cpp:53-61`），但最终没用。

### 6.3 `OnMessage` —— 分发（`rpcprovider.cpp:133`）

```cpp
void RpcProvider::OnMessage(const TcpConnectionPtr& conn, muduo::net::Buffer* buffer, Timestamp) {
  std::string recv_buf = buffer->retrieveAllAsString();    // 取走当前缓冲的全部字节

  // 解析 varint header 长度
  ArrayInputStream array_input(recv_buf.data(), recv_buf.size());
  CodedInputStream coded_input(&array_input);
  uint32_t header_size{};
  coded_input.ReadVarint32(&header_size);

  // 读 RpcHeader（用 PushLimit 限定读取边界）
  std::string rpc_header_str;
  RPC::RpcHeader rpcHeader;
  Limit msg_limit = coded_input.PushLimit(header_size);
  coded_input.ReadString(&rpc_header_str, header_size);
  coded_input.PopLimit(msg_limit);

  if (!rpcHeader.ParseFromString(rpc_header_str)) { return; }
  service_name = rpcHeader.service_name();
  method_name  = rpcHeader.method_name();
  args_size    = rpcHeader.args_size();

  // 读业务参数
  std::string args_str;
  if (!coded_input.ReadString(&args_str, args_size)) { return; }   // ← 数据不全则静默丢弃

  // 查路由表
  auto it = m_serviceMap.find(service_name);
  if (it == m_serviceMap.end()) { /* 只打印，不回复 */ return; }
  auto mit = it->second.m_methodMap.find(method_name);
  if (mit == it->second.m_methodMap.end()) { /* 只打印，不回复 */ return; }

  // 用原型克隆出具体的 request/response 类型
  Service* service = it->second.m_service;
  const MethodDescriptor* method = mit->second;
  Message* request  = service->GetRequestPrototype(method).New();
  request->ParseFromString(args_str);
  Message* response = service->GetResponsePrototype(method).New();

  // 绑定回响应回调
  Closure* done = NewCallback<RpcProvider, const TcpConnectionPtr&, Message*>(
      this, &RpcProvider::SendRpcResponse, conn, response);

  // 分发到业务
  service->CallMethod(method, nullptr, request, response, done);
}
```

**三个设计亮点**：

1. **`GetRequestPrototype(method).New()`** —— 框架事先不知道参数是什么类型，通过原型克隆拿到正确的派生类实例，再 `ParseFromString` 填充。这是"完全泛型"的关键。
2. **`PushLimit` / `PopLimit`** —— 读完 header 就恢复读取边界，避免越界读到 args 段。这个 API 用得是对的。
3. **`NewCallback` + `done->Run()`** —— 业务实现执行完（Raft 里是 `raft.cpp:930` 的 `done->Run()`）后回调 `SendRpcResponse` 把响应发回去。**框架把"何时回复"的控制权交给了业务**，这样业务可以异步回复。

**顺带说明两条奇怪之处**：
- `CallMethod` 的 `controller` 传的是 `nullptr` —— 服务端不用 controller。
- `service->CallMethod` 前的定位是靠**字符串比较**，每个请求都要查两次无序 map。热路径上有优化空间。

### 6.4 `SendRpcResponse`（`rpcprovider.cpp:238`）

```cpp
void RpcProvider::SendRpcResponse(const TcpConnectionPtr& conn, Message* response) {
  std::string response_str;
  if (response->SerializeToString(&response_str))
      conn->send(response_str);        // 直接序列化后发，没有长度前缀
  else
      std::cout << "serialize response_str error!" << std::endl;
}
```

**响应就是裸的 `response` 序列化字节，没有长度前缀、没有分帧。** 这正是[疑点 1](#10-已知疑点)的根源——发出去的时候不框定边界，收的时候也就无从判断消息在哪里结束。

`conn->shutdown()` 被注释掉了（`rpcprovider.cpp:247`），说明从短连接改成了长连接。但 `mprpcchannel.h` 里的注释还在说"使用的是短连接"。

---

## 7. Controller：错误如何穿过框架

`MprpcController`（`mprpccontroller.cpp`）只有 24 行，本质是个带 bool 的字符串容器：

```cpp
void  SetFailed(const std::string& reason) { m_failed = true; m_errText = reason; }
bool  Failed() const { return m_failed; }
std::string ErrorText() const { return m_errText; }
```

它实现了 `google::protobuf::RpcController` 的五个纯虚函数，其中三个是**空实现**：

```cpp
void StartCancel() {}                              // 未实现
bool IsCanceled() const { return false; }          // 永远返回 false
void NotifyOnCancel(Closure* callback) {}          // 未实现
```

**它的作用是"带外"传递错误**：`CallMethod` 的返回类型是 `void`，没有返回值可以表示失败，所以 protobuf 设计了 controller 这个旁路。

上层怎么用（`raftServerRpcUtil.cpp:24`）：

```cpp
bool raftServerRpcUtil::PutAppend(PutAppendArgs* args, PutAppendReply* reply) {
  MprpcController controller;                     // 每次调用新建，避免状态串扰
  stub->PutAppend(&controller, args, reply, nullptr);
  return !controller.Failed();                    // 把错误折叠成 bool
}
```

**这个折叠是有损的**：`Failed()` 只区分"RPC 层成功/失败"，丢掉了 `ErrorText()` 的具体原因。`raftServerRpcUtil::PutAppend` 里 `std::cout << controller.ErrorText()` 打了日志，但 `Get` 连日志都没打，直接丢弃。这解释了为什么客户端很难诊断 RPC 失败——**错误信息在框架和业务之间被打散了**。

**取消机制完全没实现**，意味着业务层无法主动终止一个卡住的 RPC。配合[疑点 2](#10-已知疑点)（recv 无超时），一旦卡住就只能等进程结束。

---

## 8. Config：配置文件解析

`MprpcConfig`（`mprpcconfig.cpp`）解析 `key=value` 格式：

```cpp
void MprpcConfig::LoadConfigFile(const char* config_file) {
  FILE* pf = fopen(config_file, "r");
  if (nullptr == pf) { std::cout << config_file << " is note exist!" << std::endl; exit(EXIT_FAILURE); }

  while (!feof(pf)) {
    char buf[512] = {0};
    fgets(buf, 512, pf);
    std::string read_buf(buf);
    Trim(read_buf);

    if (read_buf[0] == '#' || read_buf.empty()) continue;    // 注释 / 空行

    int idx = read_buf.find('=');
    if (idx == -1) continue;                                 // 非法行

    std::string key = read_buf.substr(0, idx);
    Trim(key);
    int endidx = read_buf.find('\n', idx);
    std::string value = read_buf.substr(idx + 1, endidx - idx - 1);
    Trim(value);
    m_configMap.insert({key, value});                        // ← insert，不是 operator[]
  }
  fclose(pf);
}

std::string MprpcConfig::Load(const std::string& key) {
  auto it = m_configMap.find(key);
  return (it == m_configMap.end()) ? "" : it->second;        // 找不到返回空串
}
```

**`Load` 返回空串表示"没有这个 key"** —— 这正是 `Clerk::Init` 和 `KvServer` 构造函数里 `if (nodeIp.empty()) break;` 的判定依据（"读到空就说明节点数到底了"）。

**`Trim` 只去掉空格**（`mprpcconfig.cpp:59`）：

```cpp
int idx = src_buf.find_first_not_of(' ');
// 只处理 ' '，不处理 '\t' 和 '\r'
```

在 CRLF 行尾的文件上会残留 `\r`。不过在 Linux 下运行、文件是 LF 的话无影响。

**三处问题**（都在[疑点](#10-已知疑点)里展开）：
- `insert` 而非 `operator[]` → 重复 key 首个生效；
- 文件不存在直接 `exit(EXIT_FAILURE)`；
- `while (!feof(pf))` 是经典反模式（`feof` 只在读失败**之后**才为真，会多跑一轮）。

---

## 9. 一个 RPC 框架最少需要什么

把 mprpc 拆开看，一个能跑的 RPC 框架其实只需要四件事：

| 能力 | mprpc 的实现 | 代码量 |
|---|---|---|
| **① 描述"调用什么"** | `RpcHeader{service_name, method_name}` + 生成代码的静态描述表 | ~30 行 |
| **② 序列化** | protobuf `SerializeToString` / `ParseFromString` | 0（白嫖） |
| **③ 传输** | muduo `TcpServer`（服务端）+ 裸 socket（客户端） | ~150 行 |
| **④ 分发** | `m_serviceMap` 字符串查表 + `CallMethod` 多态 | ~80 行 |

**这就是全部。** 没有服务发现、没有负载均衡、没有重试策略、没有超时、没有熔断、没有连接池、没有协议版本协商、没有压缩、没有鉴权、没有流控。

**教学价值恰恰在这里**：它把"RPC"这个被 gRPC / Thrift 重重包装的概念剥到了骨架。读完你会清楚地知道——

> RPC = 把"函数名 + 参数"序列化成字节 → 走网络 → 对端反序列化 → 按名字查到真函数 → 调用 → 把返回值序列化发回。

剩下的都是工程优化，不是概念创新。

**这个框架最聪明的设计是"不碰业务"**：`MprpcChannel` 和 `RpcProvider` 全程只操作 `MethodDescriptor` / `Message` / `Service` 这些抽象类型，**不知道任何具体的 service**。所以新增一个 RPC 服务只需要写 `.proto` + 重写虚函数，框架一行不用改。

代价是**全靠字符串匹配**（`service_name` / `method_name`），拼错一个字符运行时才发现，编译期没有任何保护。

---

## 10. 已知疑点

> ⚠️ 均为**静态阅读**发现，未运行验证。按严重程度排序。前两条在 raftRpcPro 说明里提过，这里展开细节。

### 1. 响应无分帧，缓冲区固定 1024 字节

**发送侧**（`rpcprovider.cpp:238`）：

```cpp
conn->send(response_str);          // 裸字节，没有长度前缀
```

**接收侧**（`mprpcchannel.cpp:103-124`）：

```cpp
char recv_buf[1024] = {0};
int recv_size = recv(m_clientFd, recv_buf, 1024, 0);      // 只 recv 一次！
if (-1 == recv_size) { ... }
if (!response->ParseFromArray(recv_buf, recv_size)) { controller->SetFailed(...); return; }
```

**请求侧有完整分帧，响应侧完全没有** —— 这个不对称是设计缺陷。三个问题叠加：

1. **只 `recv()` 一次**。TCP 是字节流，一次 `recv` 不保证拿全——消息可能分多个 segment 到达。
2. **缓冲区硬编码 1024**。超过就截断。
3. **`recv` 返回 0 未处理**。只判断了 `-1`，对端正常关闭（返回 0）时会拿空 buffer 去 `ParseFromArray`。

**触发场景**：`GetReply.Value` 约 1KB 以上。

```
ParseFromArray 失败 → controller->SetFailed() → raftServerRpcUtil::Get 返回 false
   → Clerk 认为 RPC 失败 → 换服务器重试 → 仍然失败 → 无限轮转
```

Raft 内部通信（`AppendEntriesReply` / `InstallSnapshotResponse`）响应都很小，不受影响；**受影响的是存大 value 的 KV 场景**。

### 2. socket 在多线程间共享，无锁且无超时

`MprpcChannel` 的 `m_clientFd` 是**每个 channel 一个**，而每个 channel 对应一个对端节点。注释明确了这一点（`raftRpcUtil.h:10-11`）：

> 对于一个 raft 节点来说，对于任意其他的节点都要维护一个 rpc 连接，即 MprpcChannel

但 `Raft::doHeartBeat`（`raft.cpp:305`）**每轮心跳都给每个 peer 起一个新的 detached 线程**：

```cpp
std::thread t(&Raft::sendAppendEntries, this, i, args, reply, appendNums);
t.detach();
```

心跳间隔只有 25ms。如果某个 RPC 慢（网络抖动、对端 GC 停顿），上一轮的线程还没返回，下一轮又起新线程：

```
t=0ms   : 线程A → send(fd, AE#1) → 阻塞在 recv(fd)
t=25ms  : 线程B → send(fd, AE#2) → 阻塞在 recv(fd)    ← 同一个 fd
```

两个线程抢同一个 fd 的 `recv`，**响应会错配到错误的请求**。在 Raft 里这会导致 `matchIndex` / `nextIndex` 计算错乱。`leaderSendSnapShot` 与 `doHeartBeat` 之间同样存在竞争。

**更糟的是没有超时**：`newConnect` 没设 `SO_RCVTIMEO`，`recv` 是永久阻塞的。一旦对端不回（比如[疑点 5](#10-已知疑点)的场景），`sendAppendEntries` 线程**永久卡住**——它持有的 `shared_ptr<int> appendNums` 也就永远不释放，`m_mtx` 也永远拿不到（如果卡在锁内的话）。

**这是整个项目最严重的可靠性问题**：它不表现为崩溃，而表现为"集群在压力下偶尔行为诡异"，极难复现和定位。

### 3. `rpcheader.pb.cpp` 逃过了 format.sh 的过滤

`format.sh`：

```bash
find . -regex '.*\.\(cpp\|hpp\|cu\|c\|h\)' ! -regex '.*\(pb\.h\|pb\.cc\)$' -exec clang-format -i {} \;
```

排除规则是 `pb.h` 或 `pb.cc` 结尾。而 `rpcheader.pb.cpp` 的扩展名是 **`.cpp` 不是 `.cc`** —— 匹配不上，**会被 clang-format 格式化**。

CLAUDE.md 明确写着"自动生成的 `*.pb.*` 文件被排除在格式化之外 —— 不要重新格式化它们"，但这个文件是个漏网之鱼。其他三个生成文件（`raftRPC.pb.cc` / `kvServerRPC.pb.cc` 及对应 `.pb.h`）因为扩展名正确而被排除。

**后果**：`rpcheader.pb.cpp` 可能已被重新格式化，与 protoc 的输出不再一致。重新生成 proto 时会产生格式差异，在 diff 里制造噪声。

### 4. `MprpcConfig` 用 `insert` → 重复 key 首个生效

```cpp
m_configMap.insert({key, value});   // insert 不会覆盖已存在的 key
```

仓库里 committed 的 `bin/test.conf` **实际就有重复**：

```
node0port=29016
node1port=29017        ← 第一次出现
node2port=29018
node1ip=127.0.1.1
node1port=7788         ← 第二次出现，被忽略
```

**先澄清一点**：`RpcProvider::Run` 确实用 `std::ios::app` 只追加、从不清理，但**正常的启动流程不会因此产生重复**——启动器 `raftCoreRun` 在 fork 之前会先把配置文件 truncate 掉（`example/raftCoreExample/raftKvDB.cpp:34-42`）：

```cpp
std::ofstream file(configFileName, std::ios::out | std::ios::app);
file.close();
file = std::ofstream(configFileName, std::ios::out | std::ios::trunc);   // ← 清空
if (file.is_open()) { file.close(); std::cout << configFileName << " 已清空" << std::endl; }
```

所以**每次通过 `./raftCoreRun -n 3 -f test.conf` 启动，文件都是干净的**（3 个节点各追加自己那两行，共 6 行）。

**那么 committed 的 `bin/test.conf` 里那个重复是哪来的？** 只能是绕过启动器写入的——比如直接构造 `KvServer`、用旧版本代码、或者手工编辑过。**它是个残留物，不是常规流程的产物。**

**但 `insert` 的语义问题依然是真的**：`MprpcConfig` 取第一次出现的值。这意味着**只要文件里出现任何重复 key，陈旧值会静默胜出，而且不会报错**。当前有启动器的 truncate 兜着，所以不发作；一旦有人改动启动流程、或者手工往配置里追加内容，就会以"节点连不上对端"的形式表现出来，且极难定位。

**建议**：改成 `m_configMap[key] = value;`（最后写入生效，更符合"追加新配置"的直觉），或者更好的是**遇到重复 key 时打印一条警告**——静默取值是这里最危险的部分。

### 5. 服务端找不到 service/method 时不回响应 → 客户端永久阻塞

`rpcprovider.cpp:188-202`：

```cpp
auto it = m_serviceMap.find(service_name);
if (it == m_serviceMap.end()) {
    std::cout << "服务：" << service_name << " is not exist!" << std::endl;
    /* 只打印，什么都不发 */ return;
}
auto mit = it->second.m_methodMap.find(method_name);
if (mit == it->second.m_methodMap.end()) {
    std::cout << service_name << ":" << method_name << " is not exist!" << std::endl;
    /* 只打印，什么都不发 */ return;
}
```

**协议错误时服务端保持沉默。** 客户端那边 `recv` 没有超时（[疑点 2](#10-已知疑点)），于是**永久阻塞**。正确做法是返回一个错误响应，或至少关闭连接（`conn->shutdown()`）让客户端感知。

同样的静默丢弃还发生在参数解析失败时（`rpcprovider.cpp:172-175`）：`ReadString(&args_str, args_size)` 失败直接 `return`，但**此时数据已经从 buffer 里 `retrieveAllAsString()` 取走了**——如果这是 TCP 分片导致的"数据不全"，剩下的字节到达时已经无法与前半段拼接，协议彻底错乱。

### 6. 大请求的分片问题

muduo 的 `OnMessage` 回调**不保证一次收到完整消息**——它给的是"当前到达的字节"。代码第一行就是 `buffer->retrieveAllAsString()` 把字节全部取走，然后按 `header_size` / `args_size` 解析。

如果请求被 TCP 分片（`AppendEntriesArgs` 带大量 entries 时很容易发生）：

```
t=0 : OnMessage 收到前 800 字节 → retrieveAllAsString 全部取走
      → ReadString(args_str, args_size) 失败 → return（数据已丢失）
t=1 : OnMessage 收到剩余 400 字节 → 从头解析 → 把中段当成 varint 长度 → 完全错乱
```

**没有做"数据不够就先存着、等下次回调再拼"的处理。** 这也印证了[疑点 1](#10-已知疑点)里"响应无分帧"是同一个设计缺失——**整个框架缺乏流式协议的状态机**。

### 7. `RpcProvider` 生命周期与 `detach` 线程

`KvServer` 构造里（`kvServer.cpp:388-397`）：

```cpp
std::thread t([this, port]() -> void {
    RpcProvider provider;                        // 局部对象
    provider.NotifyService(this);
    provider.NotifyService(this->m_raftNode.get());
    provider.Run(m_me, port);                    // 阻塞在 m_eventLoop.loop()
});
t.detach();
```

`provider` 是**局部对象**，线程永不退出，所以：
- `~RpcProvider()`（里面有 `m_eventLoop.quit()`）**永远不会被调用**；
- 析构函数里 `m_muduo_server->ipPort()` 的日志永远不会打印；
- 而 `m_muduo_server` 是 `shared_ptr`，也不会被释放。

功能上没问题（进程退出时操作系统回收），但这意味着**没有优雅关闭**。测试时想重启单节点只能杀进程。

### 8. `Run` 取 ip 的循环写法

```cpp
for (int i = 0; hent->h_addr_list[i]; i++)
    ipC = inet_ntoa(*(struct in_addr*)(hent->h_addr_list[i]));   // 只保留最后一个
```

循环只为了拿**最后一个**地址，前面的全被覆盖。多网卡机器上取到哪个不确定。另外 `gethostbyname` 不是线程安全的（返回静态缓冲区），不过这里在启动阶段单线程调用，无实际影响。

### 9. 错误信息被丢弃

`raftServerRpcUtil::Get`（`raftServerRpcUtil.cpp:18-22`）：

```cpp
bool raftServerRpcUtil::Get(GetArgs* args, GetReply* reply) {
  MprpcController controller;
  stub->Get(&controller, args, reply, nullptr);
  return !controller.Failed();      // ErrorText() 直接丢弃，连打印都没有
}
```

对比 `PutAppend`（同一个文件）至少打了 `std::cout << controller.ErrorText()`。**Get 失败时完全静默**，排查问题时只能靠猜。这个不对称看起来是疏忽。

### 10. `MprpcConfig` 的其他小问题

- **文件不存在直接 `exit(EXIT_FAILURE)`**（`mprpcconfig.cpp:12`）——库函数里调用 `exit` 是很粗暴的，调用方无法处理错误。
- **`while (!feof(pf))`** —— `feof` 只在读取失败后才置位，所以循环会多跑一轮（最后 `fgets` 返回 NULL，`buf` 保持全零，靠 `read_buf.empty()` 兜住）。能用，但是反模式。
- **`Trim` 只处理 `' '`**，不处理 `'\t'` 和 `'\r'`。
- **`buf[512]` 固定长度**，超长行会被 `fgets` 截断成两行，第二行没有 `=` 被丢弃。对当前配置格式无害。

---

## 11. 与工业 RPC 框架的对比

| 能力 | mprpc | gRPC / Thrift |
|---|---|---|
| **协议** | 自定义：varint 长度 + `RpcHeader` + 裸 payload | HTTP/2 + 长度前缀帧（gRPC）；自定义分帧（Thrift） |
| **响应分帧** | ❌ 无 | ✅ 有 |
| **传输** | 每对端一条裸 TCP，无复用 | HTTP/2 多路复用 / 连接池 |
| **超时** | ❌ 无 | ✅ deadline 传播 |
| **取消** | ❌ `StartCancel()` 空实现 | ✅ |
| **服务发现** | ❌ 读本地文件 | ✅ 可插拔 resolver |
| **负载均衡** | ❌ | ✅ 可插拔 |
| **拦截器 / 中间件** | ❌ | ✅ |
| **压缩** | ❌ | ✅ |
| **鉴权 / TLS** | ❌ | ✅ |
| **流式 RPC** | ❌ 只有 unary | ✅ 四种模式 |
| **代码生成** | protobuf `cc_generic_services`（已 deprecated） | 官方一等公民 |
| **错误模型** | 一个 bool + 字符串 | 状态码 + 元数据 + 错误详情 |
| **并发安全** | ❌ 见疑点 2 | ✅ |

**这张表的意义不是说 mprpc 差** —— 它是一个教学实现，643 行手写代码换来"RPC 到底是什么"的完整理解，成本收益极高。

**意义在于划清边界**：读完 mprpc，你知道了 RPC 的**骨架**；上面这些缺失项，每一项都是**工程问题**而非概念问题。这个区分很重要——很多人以为分布式系统的难点在"选哪个 RPC 框架"，实际上难点在骨架之下的共识、复制、一致性，而骨架本身早就被标准化了。

---

## 附：这个模块值得学什么

1. **`google::protobuf` 的三个抽象点**：`RpcChannel` / `RpcController` / `Service`。protobuf 只定义接口，把网络实现完全留给使用者——这是一个非常干净的扩展点设计。
2. **生成代码 + 框架 + 业务的三段式多态分发**（第 3 节的图）。三者互不知晓对方细节，是所有 RPC 框架的共同骨架。
3. **`GetRequestPrototype(method).New()` 的泛型技巧**（`rpcprovider.cpp:208`）——在不知道具体类型的情况下，通过原型克隆出正确的派生类。这是"完全泛型化"的关键一招。
4. **框架被业务污染的实例**（`RpcProvider::Run` 写 `test.conf`）——一个反面教材，说明"传输层不应该知道部署细节"。它直接导致了 CLAUDE.md 里那个"工作目录的坑"。

**读法建议**：先读 `mprpcchannel.cpp:18-125`（171 行里的核心 100 行），再读 `rpcprovider.cpp:19-39`（注册）和 `:133-235`（分发）。`rpcprovider.cpp:224-232` 那段中文注释是作者对"多态实现协议分发"的理解，写得比代码本身更清楚，值得逐字读。

**修 bug 的入口**：如果要动手改进，[疑点 1](#10-已知疑点)（响应分帧）和[疑点 2](#10-已知疑点)（并发 + 超时）是收益最高的两处，改完这个项目的可靠性会上一个台阶。疑点 4（`insert` → 赋值 + 重复告警）是一行改动，能消除一个"静默取错值"的隐患。

---

## 相关文档

- [raftRpcPro 说明](../raftRpcPro/raftRpcPro说明.md) —— 本模块驱动的那套协议定义
- [raftCore 说明](../raftCore/raftCore说明.md) —— 用这些 RPC 实现的共识算法
- [raftClerk 说明](../raftClerk/raftClerk说明.md) —— 客户端侧的 `MprpcChannel` 使用方
- [rpc 示例](../../example/rpcExample/rpc_example.md) —— 不依赖 Raft 的独立用法
- [CLAUDE.md](../../CLAUDE.md) —— 构建、运行与工作目录的坑
