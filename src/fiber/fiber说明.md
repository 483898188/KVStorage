# fiber（monsoon）模块说明

> 有栈协程库 + N:M 调度器 + epoll 事件循环 + 系统调用 hook。代号 **monsoon**。
>
> ⚠️ **这个模块与分布式算法无关。** 它是 Raft 用来跑定时器的"运行时底座"，
> 2499 行占了 `src/` 手写代码的 40%，但**对理解 Raft 没有任何帮助**。
>
> 本文的定位是**回答一个问题**：Raft 为什么要用它，以及它到底给了 Raft 什么（见[第 10 节](#10-raft-实际用了什么)）。
> 如果你只想学分布式，看第 1 节和第 10 节就够了。

## 目录

- [1. 结论先行](#1-结论先行)
- [2. 文件清单](#2-文件清单)
- [3. 三个层次](#3-三个层次)
- [4. Fiber：有栈协程](#4-fiber有栈协程)
- [5. Scheduler：N:M 调度](#5-schedulernm-调度)
- [6. IOManager：事件循环](#6-iomanager事件循环)
- [7. Hook：核心机制](#7-hook核心机制)
- [8. Timer](#8-timer)
- [9. 未被 hook 的锁](#9-未被-hook-的锁)
- [10. Raft 实际用了什么](#10-raft-实际用了什么)
- [11. 已知疑点](#11-已知疑点)
- [12. 学习建议](#12-学习建议)

---

## 1. 结论先行

**这是从 sylar C++ 高性能服务器教程改造来的协程库**，不是本项目原创。证据在头文件保护宏里：

```cpp
#ifndef __SYLAR_IOMANAGER_H__   // iomanager.hpp:1
#ifndef __SYLAR_THREAD_H_       // thread.hpp:1
#ifndef __MONSOON_MONSOON_H__   // monsoon.h:1（新的文件用了新前缀）
```

命名空间改成了 `monsoon`，但老的 `__SYLAR_*` 保护宏留在了原文件里。

**Raft 对它的使用只有三行**（`raft.cpp:1027-1034`）：

```cpp
m_ioManager = std::make_unique<monsoon::IOManager>(FIBER_THREAD_NUM, FIBER_USE_CALLER_THREAD);
m_ioManager->scheduler([this]() { this->leaderHearBeatTicker(); });
m_ioManager->scheduler([this]() { this->electionTimeOutTicker(); });
```

而 `FIBER_THREAD_NUM = 1`、`FIBER_USE_CALLER_THREAD = false`（`config.h:21-22`）。

**即：2499 行的协程库，被用来在 1 个 OS 线程上跑 2 个死循环定时器。**

用两个 `std::thread` 也能达到同样效果。协程在这里的收益是**省了两份栈 + 省了线程切换开销**，代价是整个库的复杂度和几个未 hook 的陷阱（见[第 9 节](#9-未被-hook-的锁)）。

**作者自己也在 `raft.cpp:1031-1032` 标注了这是实验性的**：

> `todo:原来是启动了三个线程，现在是直接使用了协程，三个函数中 leaderHearBeatTicker、electionTimeOutTicker 执行时间是恒定的，applierTicker 时间受到数据库响应延迟和两次 apply 之间请求数量的影响，这个随着数据量增多可能不太合理，最好其还是启用一个线程。`

---

## 2. 文件清单

| 文件 | 行数 | 说明 |
|---|---:|---|
| `hook.cpp` | 475 | **系统调用 hook**，模块最核心也最 tricky 的部分 |
| `iomanager.cpp` | 374 | epoll 事件循环 + 定时器集成 |
| `scheduler.cpp` | 203 | N:M 协程调度器 |
| `timer.cpp` | 186 | 时间轮（实际是 `std::set` 红黑树） |
| `fiber.cpp` | 150 | 协程本体 |
| `fd_manager.cpp` | 106 | fd → 上下文映射 |
| `thread.cpp` | 67 | `pthread` 封装 |
| `utils.cpp` | 9 | `GetThreadId` 等 |
| `include/mutex.hpp` | 164 | `Mutex` / `RWMutex` / RAII 锁 |
| `include/scheduler.hpp` | 146 | |
| `include/hook.hpp` | 93 | |
| `include/utils.hpp` | 90 | `CondPanic`、backtrace |
| `include/fd_manager.hpp` | 80 | |
| `include/iomanager.hpp` | 80 | |
| `include/timer.hpp` | 78 | |
| `include/fiber.hpp` | 75 | |
| `include/singleton.hpp` | 58 | 单例模板 |
| `include/thread.hpp` | 42 | |
| `include/noncopyable.hpp` | 13 | |
| `include/monsoon.h` | 10 | **对外唯一入口**，聚合 include |
| **合计** | **2499** | |

**对外接口只有 `monsoon.h`**：

```cpp
#include "fd_manager.hpp"
#include "fiber.hpp"
#include "hook.hpp"
#include "iomanager.hpp"
#include "thread.hpp"
#include "utils.hpp"
```

`Raft` 只 `#include "monsoon.h"`，然后用了 `monsoon::IOManager` 一个类。

**CMakeLists 的特别之处**：

```cmake
add_library(fiber_lib ${SRC_LIST})
target_link_libraries(fiber_lib -ldl)      # ← 因为 hook 用了 dlsym
set(src_fiber ${SRC_LIST} CACHE INTERNAL "Description of the variable")
```

`-ldl` 是必需的——hook 机制靠 `dlsym(RTLD_NEXT, ...)` 在运行时查找原始系统调用地址。

---

## 3. 三个层次

```
┌──────────────────────────────────────────────────────────┐
│ IOManager : Scheduler, TimerManager                       │
│   epoll 事件循环 + 定时器 + tickle 管道                     │
│   ┌────────────────────────────────────────────────┐     │
│   │ Scheduler                                       │     │
│   │   N 个线程，每线程 1 个调度协程，M 个任务协程      │     │
│   │   ┌──────────────────────────────────────┐     │     │
│   │   │ Fiber                                 │     │     │
│   │   │   ucontext 有栈协程                    │     │     │
│   │   └──────────────────────────────────────┘     │     │
│   └────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────┘
                          ▲
                          │ set_hook_enable(true)
                          │
              ┌───────────────────────┐
              │ hook.cpp              │
              │ 把阻塞系统调用改写成   │
              │ "注册事件 + 让出协程"  │
              └───────────────────────┘
```

**自下而上**：

- **Fiber**：一个执行流，有自己的栈，靠 `ucontext` 切换。
- **Scheduler**：把 N 个线程和 M 个协程对应起来，维护任务队列。
- **IOManager**：在 Scheduler 基础上加 epoll——空闲时不是"没事干"，而是"等 IO 事件"。
- **Hook**：让协程内的 `sleep` / `read` / `send` 自动变成"让出 + 等事件"，从而**在不改业务代码的前提下**实现非阻塞。

**这是 sylar 协程库的标准分层**，理解了这个分层，整个模块的结构就清楚了。

---

## 4. Fiber：有栈协程

### ucontext 三件套

`fiber.cpp` 的全部机制就是三个 glibc 函数：

```cpp
// ① 创建时：取当前上下文作为模板
getcontext(&ctx_);
ctx_.uc_link = nullptr;
ctx_.uc_stack.ss_sp = stack_ptr;        // 自带的栈
ctx_.uc_stack.ss_size = stackSize_;
makecontext(&ctx_, &Fiber::MainFunc, 0);   // 绑定入口函数

// ② 切进去
swapcontext(&(Scheduler::GetMainFiber()->ctx_), &ctx_);

// ③ 让出来
swapcontext(&ctx_, &(Scheduler::GetMainFiber()->ctx_));
```

**关键点**：`makecontext` 之前必须先 `getcontext` 拿到模板，再改 `uc_stack` 和入口函数。这是 ucontext API 的固定套路。

**栈是 `malloc` 出来的**（`:20-24`）：

```cpp
class StackAllocator {
  static void *Alloc(std::size_t size) { return malloc(size); }
  static void Delete(void *vp, std::size_t size) { return free(vp); }
};
static int g_fiber_stack_size = 128 * 1024;     // 默认 128KB
```

用 `malloc` 而非 `mmap` + guard page，**意味着栈溢出不会触发 SIGSEGV 而是静默踩坏相邻内存**。生产级实现（如 folly、boost.context）会用 `mmap` 加保护页。

### 协程状态

```cpp
enum State { READY, RUNNING, TERM };
```

**只有三个状态**——没有 BLOCKED。因为"阻塞"表现为"让出并挂到某个事件/定时器上"，协程自己不记录这件事。

### 主协程 vs 子协程

每个线程有一个**主协程**（`Fiber()` 无参构造，`:26-34`），它没有独立栈（`stack_ptr = nullptr`），代表"线程本身"。子协程必须由主协程切换进来。

```cpp
// 无参构造：只给主协程用
Fiber::Fiber() {
  SetThis(this);
  state_ = RUNNING;
  getcontext(&ctx_);
  ...
}
```

析构函数靠 `stack_ptr` 是否为空来区分两种协程（`:132-149`）——**这是个脆弱的设计**，用一个字段的隐式含义（空指针 = 主协程）来编码类型。

### 协程结束

```cpp
void Fiber::MainFunc() {
  Fiber::ptr cur = GetThis();
  cur->cb_();
  cur->cb_ = nullptr;
  cur->state_ = TERM;
  auto raw_ptr = cur.get();
  cur.reset();          // 释放 shared_ptr（引用计数减 1）
  raw_ptr->yield();     // 用裸指针切换回主协程——因为 cur 已经没了
}
```

**`reset()` 后用裸指针** 是必要的，也是危险的——`raw_ptr->yield()` 时对象已经可能被析构。注释里也提到了这一点。函数返回后不再触碰 `raw_ptr`，所以能工作。

### 每线程的两种切换目标

```cpp
void Fiber::resume() {
  if (isRunInScheduler_) {
    swapcontext(&(Scheduler::GetMainFiber()->ctx_), &ctx_);    // 切给调度协程
  } else {
    swapcontext(&(cur_thread_fiber->ctx_), &ctx_);             // 切给线程主协程
  }
}
```

`isRunInScheduler_` 决定切回去的目标。`Scheduler` 的 `rootFiber_` 就是以 `run_in_scheduler = false` 创建的（`scheduler.cpp:31`）——它是"调度协程"，本身不参与调度。

---

## 5. Scheduler：N:M 调度

### 两种线程角色

```cpp
Scheduler(size_t threads, bool use_caller, const std::string& name);
```

- **`use_caller = true`**：调用 `Scheduler` 构造函数的那个线程（通常 main）也加入调度池，总线程数 = `threads`。
- **`use_caller = false`**：另起 `threads` 个线程，调用者不参与。

```cpp
if (use_caller) {
  --threads;                                        // 总线程数减 1
  Fiber::GetThis();                                 // 初始化 caller 线程主协程
  cur_scheduler = this;
  rootFiber_.reset(new Fiber(std::bind(&Scheduler::run, this), 0, false));
  cur_scheduler_fiber = rootFiber_.get();
  rootThread_ = GetThreadId();
  threadIds_.push_back(rootThread_);
}
threadCnt_ = threads;
```

**`rootFiber_` 只在 `use_caller = true` 时创建**——它是 caller 线程上的调度协程。

### 调度循环 `run()`

```cpp
void Scheduler::run() {
  set_hook_enable(true);                 // ← 关键：本线程开启 hook
  setThis();
  if (GetThreadId() != rootThread_)
    cur_scheduler_fiber = Fiber::GetThis().get();      // 工作线程的调度协程

  Fiber::ptr idleFiber(new Fiber(std::bind(&Scheduler::idle, this)));
  Fiber::ptr cbFiber;

  while (true) {
    // ① 从任务队列取一个归本线程的任务
    {
      Mutex::Lock lock(mutex_);
      auto it = tasks_.begin();
      while (it != tasks_.end()) {
        if (it->thread_ != -1 && it->thread_ != GetThreadId()) { ++it; tickle_me = true; continue; }
        task = *it; tasks_.erase(it++); ++activeThreadCnt_; break;
      }
      tickle_me |= (it != tasks_.end());
    }
    if (tickle_me) tickle();

    // ② 执行
    if (task.fiber_) {
      task.fiber_->resume();              // 切进去，跑到它 yield 或结束
      --activeThreadCnt_;
    } else if (task.cb_) {
      // 把函数包成协程执行，并复用 cbFiber 的栈
      if (cbFiber) cbFiber->reset(task.cb_);
      else cbFiber.reset(new Fiber(task.cb_));
      cbFiber->resume();
      --activeThreadCnt_;
      cbFiber.reset();
    } else {
      // ③ 没任务 → 跑 idle
      if (idleFiber->getState() == Fiber::TERM) break;
      ++idleThreadCnt_;
      idleFiber->resume();
      --idleThreadCnt_;
    }
  }
}
```

**这是标准的"协程工作窃取（其实是共享队列）"模型**：

- `resume()` 是**非阻塞**的——它切进去，协程跑到下一个 yield 点就切回来。所以 `run()` 的循环不会因为某个协程卡住而停摆。
- 没有任务时跑 `idleFiber`，`IOManager` 重写了 `idle()` 来 `epoll_wait`。
- `activeThreadCnt_` / `idleThreadCnt_` 是原子计数，用于 `stopping()` 判断和 tickle 决策。

**任务可以是协程或函数**（`SchedulerTask` 有两个成员 `fiber_` / `cb_`，二者其一非空）：

```cpp
SchedulerTask(Fiber::ptr f, int t) : fiber_(f), thread_(t) {}
SchedulerTask(std::function<void()> f, int t) { cb_ = f; thread_ = t; }
```

`Raft` 传的是 lambda，所以走 `cb_` 分支——每次调度会把它包成一个协程（第一次新建，之后复用 `cbFiber`）。

**`thread_` 参数**：可以指定任务只能在某个线程上跑（`-1` 表示不限）。`Raft` 传的是默认的 `-1`。

### `idle()` 与 `stopping()`

```cpp
bool Scheduler::stopping() {
  Mutex::Lock lock(mutex_);
  return isStopped_ && tasks_.empty() && activeThreadCnt_ == 0;
}

void Scheduler::idle() {
  while (!stopping()) {
    Fiber::GetThis()->yield();          // 让回 run()，让它再找找有没有任务
  }
}
```

**`idle()` 是一个"忙等"协程**——它 `yield` 回 `run()`，`run()` 发现没任务，又 `resume` 它……如此循环。`IOManager` 覆盖了 `idle()`，在里面 `epoll_wait` 阻塞，才避免了 CPU 空转。

**裸 `Scheduler` 的 `idle()` 会 100% 占满 CPU**——这是个只适合配合 `IOManager` 使用的基类。

### `stop()`

```cpp
void Scheduler::stop() {
  if (stopping()) return;
  isStopped_ = true;
  if (isUseCaller_) CondPanic(GetThis() == this, "cur thread is not caller thread");
  else              CondPanic(GetThis() != this, "cur thread is caller thread");

  for (size_t i = 0; i < threadCnt_; i++) tickle();   // 唤醒所有线程让它们退出
  if (rootFiber_) tickle();

  if (rootFiber_) rootFiber_->resume();               // caller 线程切到调度协程，开始跑 run()

  std::vector<Thread::ptr> threads;
  { Mutex::Lock lock(mutex_); threads.swap(threadPool_); }
  for (auto& i : threads) i->join();
}
```

**`use_caller = true` 时，`run()` 是在 `stop()` 里才第一次被执行的**（`rootFiber_->resume()`）——因为 caller 线程的 `rootFiber_` 一直没被调度过。注释写得很清楚：

> 使用 caller 线程，则调度线程依赖 stop() 来执行 caller 线程的调度协程
> 不使用 caller 线程，只用 caller 线程去调度，则调度器真正开始执行的位置是 stop()

**这也是一个反直觉的设计**：`start()` 只启动线程池，caller 线程的调度要等 `stop()`。在本项目里 `use_caller = false`，所以这条路径不走。

---

## 6. IOManager：事件循环

### 构造

```cpp
IOManager::IOManager(size_t threads, bool use_caller, const std::string& name)
    : Scheduler(threads, use_caller, name) {
  epfd_ = epoll_create(5000);
  int ret = pipe(tickleFds_);

  // 把管道读端注册到 epoll，用于唤醒 idle
  epoll_event event{};
  event.events = EPOLLIN | EPOLLET;
  event.data.fd = tickleFds_[0];
  fcntl(tickleFds_[0], F_SETFL, O_NONBLOCK);
  epoll_ctl(epfd_, EPOLL_CTL_ADD, tickleFds_[0], &event);

  contextResize(32);
  start();                     // ← 在构造函数末尾就启动线程池
}
```

**`tickleFds_` 是一个自管道（self-pipe）**，这是 epoll 编程的经典技巧：想从"别的线程"唤醒一个阻塞在 `epoll_wait` 的线程时，没有直接的 API，于是往管道写一个字节，让 `epoll_wait` 返回。

**注意 `IOManager` 构造函数里就调用了 `start()`**——也就是说对象一创建，线程池就跑起来了。`Raft::init` 里 `make_unique<IOManager>(...)` 之后立刻 `scheduler(...)`，就是这个原因。

### `addEvent` / `delEvent` / `cancelEvent`

三个函数的区别：

| 函数 | 行为 |
|---|---|
| `addEvent` | 注册事件，回调可以是 `cb` 或**当前协程** |
| `delEvent` | 注销事件，**不触发**回调 |
| `cancelEvent` | 注销事件，**先触发**回调 |
| `cancelAll` | 注销该 fd 的全部事件，逐个触发 |

**`addEvent` 的核心**（`iomanager.cpp:106-117`）：

```cpp
EventContext& event_ctx = fd_ctx->getEveContext(event);
event_ctx.scheduler = Scheduler::GetThis();
if (cb) {
  event_ctx.cb.swap(cb);
} else {
  // 没传回调 → 把【当前协程】存进去
  event_ctx.fiber = Fiber::GetThis();
  CondPanic(event_ctx.fiber->getState() == Fiber::RUNNING, ...);
}
```

**"没传回调就把当前协程记下来"** 是 hook 机制的基础：hook 里的 `addEvent(fd, event)` 不传回调，于是当前协程被登记，等事件就绪时被 `scheduler()` 重新放回任务队列——**协程就在 `yield()` 处恢复**。

### `idle()`：epoll 主循环

```cpp
void IOManager::idle() {
  const uint64_t MAX_EVENTS = 256;
  epoll_event* events = new epoll_event[MAX_EVENTS]();

  while (true) {
    uint64_t next_timeout = 0;
    if (stopping(next_timeout)) break;              // 停止条件

    // 阻塞等待（超时时间取 min(下个定时器, 5000ms)）
    int ret;
    do {
      static const int MAX_TIMEOUT = 5000;
      if (next_timeout != ~0ull) next_timeout = std::min((int)next_timeout, MAX_TIMEOUT);
      else                       next_timeout = MAX_TIMEOUT;
      ret = epoll_wait(epfd_, events, MAX_EVENTS, (int)next_timeout);
      if (ret < 0) { if (errno == EINTR) continue; break; }
      else break;
    } while (true);

    // 处理超时定时器
    std::vector<std::function<void()>> cbs;
    listExpiredCb(cbs);
    for (const auto& cb : cbs) scheduler(cb);
    cbs.clear();

    // 处理 IO 事件
    for (int i = 0; i < ret; i++) {
      epoll_event& event = events[i];
      if (event.data.fd == tickleFds_[0]) {
        // 被 tickle 唤醒，把管道读空
        uint8_t dummy[256];
        while (read(tickleFds_[0], dummy, sizeof(dummy)) > 0);
        continue;
      }
      FdContext* fd_ctx = (FdContext*)event.data.ptr;
      Mutex::Lock lock(fd_ctx->mutex);

      if (event.events & (EPOLLERR | EPOLLHUP))
        event.events |= (EPOLLIN | EPOLLOUT) & fd_ctx->events;

      int real_events = NONE;
      if (event.events & EPOLLIN)  real_events |= READ;
      if (event.events & EPOLLOUT) real_events |= WRITE;
      if ((fd_ctx->events & real_events) == NONE) continue;

      // 剩余事件重新注册
      int left_events = (fd_ctx->events & ~real_events);
      int op = left_events ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;
      event.events = EPOLLET | left_events;
      epoll_ctl(epfd_, op, fd_ctx->fd, &event);

      // 触发（把协程/回调放回任务队列）
      if (real_events & READ)  { fd_ctx->triggerEvent(READ);  --pendingEventCnt_; }
      if (real_events & WRITE) { fd_ctx->triggerEvent(WRITE); --pendingEventCnt_; }
    }

    // 让出，回到 run() 去执行刚入队的任务
    Fiber::ptr cur = Fiber::GetThis();
    auto raw_ptr = cur.get();
    cur.reset();
    raw_ptr->yield();
  }
}
```

**整个事件循环的骨架**：`epoll_wait` 阻塞 → 有事件或超时 → 把就绪的协程/定时器回调**重新投入任务队列** → `yield` 回 `run()` → `run()` 取出任务并 `resume`。

**注意用的是 ET（边缘触发）**：`EPOLLET` 到处都在用，配合 fd 设为非阻塞。这是高性能 IO 的标准组合，但也意味着**必须一次读完所有数据**，否则会丢事件。

**`pendingEventCnt_`** 追踪未处理的 IO 事件数，`stopping()` 要求它为 0 才允许退出。

---

## 7. Hook：核心机制

**这是整个库最有技术含量、也最容易出问题的部分。**

### 原理

`hook.cpp:36-45` 在**程序启动前**（静态对象构造）用 `dlsym(RTLD_NEXT, ...)` 拿到 libc 里原始函数的地址：

```cpp
void hook_init() {
  static bool is_inited = false;
  if (is_inited) return;
  #define XX(name) name##_f = (name##_fun)dlsym(RTLD_NEXT, #name);
  HOOK_FUN(XX);
  #undef XX
}

struct _HOOKIniter { _HOOKIniter() { hook_init(); ... } };
static _HOOKIniter s_hook_initer;      // ← 全局对象，main 之前构造
```

然后**定义同名函数**覆盖 libc 的符号：

```cpp
extern "C" {
  #define XX(name) name##_fun name##_f = nullptr;
  HOOK_FUN(XX);
  #undef XX

  unsigned int sleep(unsigned int seconds) { ... }
  int usleep(useconds_t usec) { ... }
  ...
}
```

因为可执行文件自身的符号优先于 `libc.so`，所以**只要链接了这个库，全进程的 `sleep` / `read` / `send` 都变成 hook 版本**。`-ldl` 就是为 `dlsym` 而加的。

### 被 hook 的 21 个函数（`hook.cpp:13-34`）

```
sleep usleep nanosleep                     ← 睡眠
socket connect accept                      ← 连接
read readv recv recvfrom recvmsg           ← 读
write writev send sendto sendmsg           ← 写
close fcntl ioctl getsockopt setsockopt    ← 杂项
```

### 线程级开关

```cpp
static thread_local bool t_hook_enable = false;   // 默认关闭

bool is_hook_enable() { return t_hook_enable; }
void set_hook_enable(const bool flag) { t_hook_enable = flag; }
```

**默认关闭**，只有 `Scheduler::run()` 里显式打开：

```cpp
void Scheduler::run() {
  set_hook_enable(true);      // ← 只有调度器线程的协程内才会被 hook
  ...
}
```

**这个设计很重要**：主线程、muduo 的网络线程、`Raft` 起的 `std::thread` 都不开 hook，所以它们的 `sleep` / `recv` 是**真的阻塞**。只有跑在调度器线程上的协程才享受"阻塞自动变让出"。

这解释了 `KvServer` 构造函数里的 `sleep(6)`（`kvServer.cpp:402`）为什么是真睡 6 秒——它在主线程上，没开 hook。

### `usleep` 的改写

```cpp
int usleep(useconds_t usec) {
  if (!t_hook_enable) return usleep_f(usec);          // 没开 → 真睡

  Fiber::ptr fiber = Fiber::GetThis();
  IOManager* iom = IOManager::GetThis();
  iom->addTimer(usec / 1000,
      std::bind((void(Scheduler::*)(Fiber::ptr, int))&IOManager::scheduler, iom, fiber, -1));
  Fiber::GetThis()->yield();                          // ← 让出
  return 0;
}
```

**"睡 X 毫秒"被翻译成"注册一个 X 毫秒后的定时器，把当前协程塞进去，然后让出"**。定时器到期时，`IOManager::idle` 会 `listExpiredCb` 取出回调并 `scheduler(cb)` ——协程回到任务队列，被 `run()` 重新 `resume`。

**这就是 Raft 的两个 ticker 能在 1 个线程上共存的原因**：`electionTimeOutTicker` 里那句 `usleep(suitableSleepTime)` 实际是让出，于是 `leaderHearBeatTicker` 得以运行。

### `do_io` 的通用改写（`hook.cpp:67-137`）

读写类 hook（`read` / `recv` / `send` / `write`…）走同一个模板：

```cpp
template <typename OriginFun, typename... Args>
static ssize_t do_io(int fd, OriginFun fun, const char* name, uint32_t event, int timeout_so, Args&&... args) {
  if (!t_hook_enable) return fun(fd, std::forward<Args>(args)...);

  FdCtx::ptr ctx = FdMgr::GetInstance()->get(fd);
  if (!ctx) return fun(...);                 // 不是被管理的 fd → 直接调用
  if (ctx->isClose()) { errno = EBADF; return -1; }
  if (!ctx->isSocket() || ctx->getUserNonblock()) return fun(...);   // 普通文件/用户已设非阻塞 → 直接调用

  uint64_t to = ctx->getTimeout(timeout_so);
  std::shared_ptr<timer_info> tinfo(new timer_info);

retry:
  ssize_t n = fun(fd, std::forward<Args>(args)...);
  while (n == -1 && errno == EINTR) n = fun(...);       // 处理信号中断

  if (n == -1 && errno == EAGAIN) {                     // 数据没准备好
    IOManager* iom = IOManager::GetThis();
    Timer::ptr timer;
    std::weak_ptr<timer_info> winfo(tinfo);
    if (to != (uint64_t)-1) {
      timer = iom->addConditionTimer(to, [winfo, fd, iom, event, name]() {
        auto t = winfo.lock();
        if (!t || t->cnacelled) return;
        t->cnacelled = ETIMEDOUT;
        iom->cancelEvent(fd, (Event)event);
      }, winfo);
    }
    int rt = iom->addEvent(fd, (Event)event);
    if (rt) {                                         // 注册失败
      if (timer) timer->cancel();
      return -1;
    } else {
      Fiber::GetThis()->yield();                      // ← 让出，等事件或超时
      if (timer) timer->cancel();                     // 醒了，取消超时定时器
      if (tinfo->cnacelled) { errno = tinfo->cnacelled; return -1; }   // 超时了
      goto retry;                                     // 事件到了，再试一次
    }
  }
  return n;
}
```

**模式很清晰**：先直接调用，遇到 `EAGAIN` 就注册事件 + 超时定时器 + 让出；醒来后 `goto retry` 重试。**因为要重试，所以 fd 必须是非阻塞的**——这也解释了 `hook.cpp:208` 里 socket 创建时自动设置 `O_NONBLOCK`。

**`FdCtx` / `FdMgr` 的作用**：按 fd 记录"是否是 socket""是否用户主动设了非阻塞""超时时间"。只有 socket 且非用户设的非阻塞 fd 才走 hook 路径——普通文件 IO 不适用 epoll，直接调用原函数。

---

## 8. Timer

### 定时器集合

```cpp
std::set<Timer::ptr, Timer::Comparator> timers_;    // 按 next_ 排序，最早的在 begin()
```

**不是时间轮，是红黑树（`std::set`）**。注释里有时叫"时间堆"，实际是 `std::set` 按到期时间排序，`*timers_.begin()` 就是最近的一个。

`Comparator` 按 `next_` 比较（`timer.cpp` 底部）：

```cpp
bool Timer::Comparator::operator()(const Timer::ptr& lhs, const Timer::ptr& rhs) const {
  if (!lhs && !rhs) return false;
  if (!lhs) return true;
  if (!rhs) return false;
  if (lhs->next_ < rhs->next_) return true;
  if (lhs->next_ > rhs->next_) return false;
  return lhs.get() < rhs.get();          // 用地址打破平局，保证严格弱序
}
```

**最后那行是必要的**——`std::set` 要求严格弱序，只比 `next_` 会让到期时间相同的两个定时器被判为"相等"从而丢失一个。

### 时间源

```cpp
static uint64_t GetElapsedMS() {                        // utils.hpp:23
  struct timespec ts = {0};
  clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
  return ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}
```

用 `CLOCK_MONOTONIC_RAW`——**单调时钟**，不受系统时间调整影响。这是定时器的正确做法。

但 `TimerManager::detectClockRolllover()`（`timer.cpp:173`）还在检测时钟回拨：

```cpp
if (now_ms < previouseTime_ && now_ms < (previouseTime_ - 60*60*1000)) rollover = true;
```

用单调时钟的话永远不会回拨，这段是死代码——大概是照抄 sylar 的（sylar 可能用的是墙钟）。

### 与 epoll 的结合

`getNextTimer()` 返回"距离最近定时器还有多少毫秒"，`IOManager::idle` 用它作为 `epoll_wait` 的超时：

```cpp
timeout = getNextTimer();
ret = epoll_wait(epfd_, events, MAX_EVENTS, (int)min(next_timeout, 5000));
```

**这样一个线程既等 IO 又等定时器**——没有独立的 timerfd 或定时线程。

`OnTimerInsertedAtFront()` 在"新定时器插到了队首"时被调用，`IOManager` 把它实现为 `tickle()`——因为新定时器可能比当前 `epoll_wait` 的剩余时间更早到期，需要唤醒重算。

---

## 9. 未被 hook 的锁

**这是理解"为什么 Raft 用协程是危险的"的关键。**

`mutex.hpp` 里的 `Mutex` / `RWMutex` 是 **`pthread_mutex_t` / `pthread_rwlock_t` 的薄封装**：

```cpp
class Mutex : Nonecopyable {
  void lock() { CondPanic(0 == pthread_mutex_lock(&m_), "lock error"); }
  ...
  pthread_mutex_t m_;
};
```

**`pthread_mutex_lock` 不在 hook 列表里。** 这意味着：

> 如果一个协程在**持有 `Mutex` 的情况下让出**（比如在锁内调用了被 hook 的 `usleep` / `recv`），
> 而**另一个协程**尝试获取同一把锁，那么第二个协程会**真正阻塞整个 OS 线程**。
> 由于两个协程跑在同一个线程上，第一个协程永远得不到 CPU 来释放锁 → **死锁**。

**这就是"协程 + 未 hook 的锁"的经典陷阱。**

`IOManager` 内部的 `FdContext::mutex` 和 `Scheduler::mutex_` 都是这种未 hook 的 `Mutex`。**库自身在多线程环境下是安全的**（每个调度器线程持锁时不会让出），但**使用者必须自己保证不在锁内让出**。

### Raft 是否踩了这个坑？

**目前没有，但是踩在边缘上。** 检查 Raft 的锁使用：

| 位置 | 是否在持锁时可能让出 |
|---|---|
| `AppendEntries1` / `RequestVote` / `InstallSnapshot` | ❌ 运行在 muduo 的 RPC 线程，**未开 hook**，不会让出 |
| `doElection` / `doHeartBeat` | ❌ 持锁期间只有 CPU 计算和起 `std::thread`，无 IO / 无 sleep |
| `electionTimeOutTicker` / `leaderHearBeatTicker` | ✅ **`usleep` 在锁外调用**（锁只包住时间计算那几行） |
| `leaderSendSnapShot` | ✅ **作者显式在 RPC 前解锁**（`raft.cpp:538`），RPC 完再锁回来 |
| `Raft::Start` | ⚠️ 持锁调用 `persist()`（文件 IO）。但 `Start` 由 RPC 线程调用，未开 hook |

**前四条都没问题**——但这依赖"锁内不做被 hook 的操作"这个**没有被任何机制强制的约定**。

`leaderSendSnapShot` 的写法说明作者意识到了这件事：

```cpp
m_mtx.lock();
// 读快照元信息到 args
m_mtx.unlock();                    // ← 显式解锁
bool ok = m_peers[server]->InstallSnapshot(&args, &reply);    // 锁外做 RPC
m_mtx.lock();                      // ← 再锁回来
```

**但这个约定靠人是靠不住的**——任何一次"为了图方便在锁内加个日志/加个 IO"的改动，都可能引入偶发死锁，而且极难复现。

**安全的做法**是用协程感知的锁（比如 `monsoon::Mutex` 应该被改造成"拿不到锁就挂起当前协程"），或者干脆让 Raft 的 ticker 用真线程。**当前实现选了最省事但最脆的一条路。**

---

## 10. Raft 实际用了什么

回到最重要的问题。**Raft 用到的 API 总共三个**：

```cpp
// raft.cpp:1027
m_ioManager = std::make_unique<monsoon::IOManager>(FIBER_THREAD_NUM, FIBER_USE_CALLER_THREAD);

// raft.cpp:1033-1034
m_ioManager->scheduler([this]() { this->leaderHearBeatTicker(); });
m_ioManager->scheduler([this]() { this->electionTimeOutTicker(); });

// raft.cpp:1036-1037（注意：这是 std::thread，不是协程）
std::thread t3(&Raft::applierTicker, this);
t3.detach();
```

### 实际运行形态

配置是 `FIBER_THREAD_NUM = 1`、`FIBER_USE_CALLER_THREAD = false`：

```
进程
├─ main 线程          → KvServer 构造函数 → 最后 join 在 ReadRaftApplyCommandLoop
├─ muduo 线程 ×4       → RpcProvider 处理 RPC（setThreadNum(4)）
├─ IOManager 工作线程 ×1  ← 这里跑【两个协程】
│    ├─ 协程 A: leaderHearBeatTicker   （while(true)，doHeartBeat 里起 std::thread）
│    └─ 协程 B: electionTimeOutTicker  （while(true)，doElection 里起 std::thread）
├─ applierTicker 线程   → 独立 std::thread，不是协程
└─ 各种临时线程         → doHeartBeat / doElection / sendRequestVote 里 detach 出来的
```

### 协程在这里做了什么

**只有一件事**：让 `leaderHearBeatTicker` 和 `electionTimeOutTicker` 两个死循环在**同一个 OS 线程**上交替执行。

它们大部分时间都花在 `usleep` 上，而 `usleep` 被 hook 成了"让出 + 定时器"。于是：

```
协程 A: doHeartBeat() → usleep(25ms) → 让出
                                    ↓
协程 B:                       → doElection 检查 → usleep(300~500ms) → 让出
                                    ↓
协程 A: ← 定时器到期，被重新调度
```

### 收益与代价

| | 用协程（现状） | 用两个 `std::thread` |
|---|---|---|
| 栈占用 | 2 × 128KB | 2 × 8MB（默认） |
| 切换开销 | `swapcontext`，用户态 | 内核态上下文切换 |
| 复杂度 | +2499 行协程库 | 0 |
| 风险 | 未 hook 的锁（第 9 节） | 无 |
| 与 Raft 算法相关性 | **无** | **无** |

**换算一下**：省下约 16MB 虚拟内存和几十微秒的切换延迟——**而这个"收益"被 `doHeartBeat` / `doElection` 里 detach 出去的 `std::thread` 抵消了大半**。每轮心跳对一个 peer 起一个真线程（`raft.cpp:305`），选举时对每个 peer 起一个真线程（`raft.cpp:240`）。**协程只省了两个定时器的线程开销，而 Raft 自己在别处疯狂起线程。**

所以作者那句 `todo` 的意思很清楚：**这是一次"用得上但没必要"的技术引入**。

### 对学分布式的人意味着什么

**你可以完全跳过这个模块，不影响理解 Raft 的任何一行逻辑。** 把 `raft.cpp:1027-1034` 换成：

```cpp
std::thread t1(&Raft::leaderHearBeatTicker, this); t1.detach();
std::thread t2(&Raft::electionTimeOutTicker, this); t2.detach();
```

Raft 的行为**完全不变**（`raft.cpp:1039-1046` 的注释里就保留着这个旧版本）。**唯一的变化**是 `electionTimeOutTicker` 里那两个 `std::cout` 打印的"设置睡眠时间/实际睡眠时间"会变成真实的睡眠——因为它不再被 hook 成让出。

> 顺带一提：那两个带 ANSI 紫色转义的 `std::cout`（`raft.cpp:346-350`、`498-515`）**每轮心跳都打印**，且不受 `Debug` 开关控制。这和 skipList 里无条件 `std::cout` 是同一类问题（skipList 说明疑点 4）。

---

## 11. 已知疑点

> ⚠️ 均为**静态阅读**发现。这个库在本项目中被极简使用（只用 IOManager + scheduler），所以多数疑点**不会被触发**，列出是为了完整性。

### 1. `Mutex` 未 hook —— 锁内让出即死锁

见[第 9 节](#9-未被-hook-的锁)。这是本模块**最重要**的一条，Raft 目前没踩到，但约定未被强制。

### 2. `Scheduler::idle()` 基类版本会 100% 占满 CPU

```cpp
void Scheduler::idle() {
  while (!stopping()) Fiber::GetThis()->yield();
}
```

`yield` → `run()` 找任务 → 没任务 → 再 `resume` 它 → 再 `yield`……**纯忙等**。只有 `IOManager` 覆盖后的版本（`epoll_wait` 阻塞）才是合理的。裸用 `Scheduler` 会烧 CPU。

### 3. 每轮心跳起一个 `std::thread`

这不是 `fiber` 模块的问题，而是 **Raft 使用它的方式**：`doHeartBeat`（`raft.cpp:305`）和 `doElection`（`raft.cpp:240`）对每个 peer 起一个 detached 线程。

心跳间隔 25ms、3 节点 → **每秒约 80 次线程创建/销毁**。线程创建涉及 `clone()` 系统调用，开销远大于协程切换。**这与引入协程的初衷直接矛盾**（第 10 节）。

叠加 mprpc 的 socket 竞态问题（[rpc 说明疑点 2](../rpc/rpc说明.md)），这是本项目的性能与稳定性主要来源。

### 4. `Nanosleep` hook 的毫秒换算错误

```cpp
int nanosleep(const struct timespec* req, struct timespec* rem) {
  ...
  int timeout_s = req->tv_sec * 1000 + req->tv_nsec / 1000 / 1000;
  iom->addTimer(timeout_s, ...);
}
```

`req->tv_nsec` 是纳秒，转毫秒应该 `/ 1000000`。写成 `/ 1000 / 1000` 是 `/ 1000000`——**数值上是对的**，只是写法绕。不过变量名叫 `timeout_s` 而实际单位是毫秒，有误导。

真正的问题是**精度损失**：亚毫秒的 `nanosleep` 会被截断成 0 毫秒。对本项目无影响（没人调用）。

### 5. `_HOOKIniter` 静态对象的初始化顺序

```cpp
static _HOOKIniter s_hook_initer;      // hook.cpp:57
```

它依赖 `hook_init()` 里的 `dlsym`。如果**其他翻译单元的静态对象在其之前构造**并调用了被 hook 的函数，`*_f` 还是 `nullptr` → 崩溃。这是"静态初始化顺序"的经典问题，在跨 TU 时无法保证。

本项目里 `Raft` 的 IOManager 是运行时创建的，不涉及，但属于隐患。

### 6. `Fiber::reset()` 只能用于 `TERM` 状态

```cpp
void Fiber::reset(std::function<void()> cb) {
  CondPanic(state_ == TERM, "state isn't TERM");
  ...
}
```

且 `Scheduler::run` 里 `cbFiber.reset()` 之后才复用。语义是"复用已结束协程的栈"。这限制了协程池的实现方式——不能回收 `READY` 状态的协程。

### 7. `idle()` 里 `next_timeout` 的 `~0ull` 判断

```cpp
if (next_timeout != ~0ull) {
  next_timeout = std::min((int)next_timeout, MAX_TIMEOUT);
} else {
  next_timeout = MAX_TIMEOUT;
}
```

`next_timeout` 是 `uint64_t`，却 `(int)` 强转后传给 `std::min`——如果 `next_timeout` 超过 `INT_MAX`（约 24 天），强转会溢出成负数。实际不会发生（有 `MAX_TIMEOUT` 兜底），但在 32 位或极端情况下是隐患。

### 8. 无优雅关闭

`IOManager::~IOManager()` 才调 `stop()`，而 `stop()` 会 `join` 所有线程。但在本项目里 `Raft` 的 `m_ioManager` 是一个 `unique_ptr` 成员，`Raft` 又是 `KvServer` 的成员，**而 `KvServer` 的构造函数永不返回**（join 在 `ReadRaftApplyCommandLoop`）——**析构链永远不会触发**，`stop()` 永远不执行，线程池线程带着 detached 状态活到进程结束。

这符合"进程退出由 OS 回收"的实用主义，但也意味着 `Scheduler::~Scheduler()` 里那句 `CondPanic(isStopped_, "isstopped is false")` 一旦被执行就会 abort。

### 9. 大量无条件的 `std::cout`

`scheduler.cpp` / `fiber.cpp` / `iomanager.cpp` 里密布调试输出，且**全部无条件**：`"[fiber] create fiber , id = "`、`"-------scheduler init success-------"`、`"tickle"`、`"add event success,fd = "` 等等。

多数被注释掉了，但 `fiber.cpp:32`、`scheduler.cpp:42` 等仍在输出。这些都不受 `Debug` 开关控制，只能改源码。**同一个模式在 skipList 里也出现过。**

### 10. `CondPanic` 在 Release 下失效

```cpp
static void CondPanic(bool condition, std::string err) {
  if (!condition) {
    std::cout << "[assert by] ..." << err << std::endl;
    std::cout << "[backtrace]\n" << BacktraceToString(6, 3, "") << std::endl;
    assert(condition);      // ← NDEBUG 下是空操作
  }
}
```

`assert` 在 `-DNDEBUG` 下被编译掉。当前 `CMAKE_BUILD_TYPE = Debug`，没有 `-DNDEBUG`，所以会 abort。但**一旦以 Release 构建，所有 `CondPanic` 都变成"打印一行然后继续执行"**——包括那些本该致命的状态检查（如 `Fiber::resume` 的状态断言、`Mutex` 的加锁失败）。

`std::cout` 本身也不是线程安全的（多个 `<<` 之间可能交错），跨线程的 `CondPanic` 输出可能混杂。

---

## 12. 学习建议

### 如果你的目标是学分布式 —— 跳过

这个模块**对理解 Raft 零贡献**。用两个 `std::thread` 替换掉（第 10 节），Raft 行为不变，代码还少 2499 行。

### 如果你想学"协程/高性能服务器" —— 值得读

它是一份**完整的、能跑的、代码量适中的**有栈协程实现。值得学的点：

1. **hook 机制**（第 7 节）——"不改业务代码实现非阻塞"的核心手法，sylar / libco / 微信的协程库都用这一套。理解了 `dlsym(RTLD_NEXT)` 覆盖符号 + `EAGAIN` 后让出重试，就理解了 C++ 有栈协程的工程本质。
2. **ucontext 三件套**（第 4 节）——`getcontext` / `makecontext` / `swapcontext`，比汇编切换容易理解得多。
3. **N:M 调度的骨架**（第 5 节）——任务队列 + idle 协程 + 原子计数。
4. **epoll + 自管道唤醒**（第 6 节）——跨线程唤醒 `epoll_wait` 的标准技巧。

### 如果只想快速理解它在干什么

读这四段就够（共约 120 行）：

| 位置 | 内容 |
|---|---|
| `hook.cpp:149-190` | `sleep` / `usleep` 的改写——**这是全部魔法的所在** |
| `hook.cpp:67-137` | `do_io` 模板——读写类 hook 的通用模式 |
| `fiber.cpp:69-99` | `resume` / `yield`——协程切换 |
| `iomanager.cpp:238-350` | `idle()`——事件循环主干 |

**不要从 `scheduler.cpp` 开始读**——`use_caller` 的两条路径、`rootFiber_`、`stop()` 里才启动 caller 调度，这些反直觉的设计会让人卡住，而本项目根本不用（`use_caller = false`）。

---

## 相关文档

- [raftCore 说明](../raftCore/raftCore说明.md) —— 第 13 节讲了 Raft 的三个定时器如何协作
- [rpc 说明](../rpc/rpc说明.md) —— mprpc 的 socket 与 hook 的关系（mprpc 用裸 socket，运行在未 hook 的线程上）
- [skipList 说明](../skipList/skipList说明.md) —— 同样是无条件 `std::cout` 的问题
- [CLAUDE.md](../../CLAUDE.md) —— `FIBER_THREAD_NUM` 等参数在 `config.h`
