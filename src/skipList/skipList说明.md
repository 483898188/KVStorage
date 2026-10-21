# skipList 模块说明

> 跳表实现，在这个项目里扮演**上层状态机**的角色——Raft 提交的日志最终都应用到这里。
>
> 本文是 [raftCore 说明](../raftCore/raftCore说明.md) 的补充。那份讲"日志如何变成 `ApplyMsg`"，
> 这份讲"`ApplyMsg` 最终落到什么数据结构上"，以及这个数据结构与快照机制的耦合。
>
> ⚠️ 本文发现的疑点里有**一个数据正确性缺陷**，见[疑点 1](#8-已知疑点)。

## 目录

- [1. 模块定位](#1-模块定位)
- [2. 文件清单](#2-文件清单)
- [3. 跳表的数据结构](#3-跳表的数据结构)
- [4. 三种核心操作](#4-三种核心操作)
- [5. `insert_set_element`：唯一的写入口](#5-insert_set_element唯一的写入口)
- [6. 快照接口](#6-快照接口)
- [7. 与 KvServer 的耦合](#7-与-kvserver-的耦合)
- [8. 已知疑点](#8-已知疑点)
- [9. 作为"可替换状态机"的评估](#9-作为可替换状态机的评估)
- [10. 跳表本身值得学什么](#10-跳表本身值得学什么)

---

## 1. 模块定位

CLAUDE.md 对这个模块的定义：

> `src/skipList` —— 仅头文件的跳表，即存储引擎 / 上层状态机（`SkipList<std::string, std::string>`）。
> **理论上可替换为任意 KV 存储。**

这句话点明了它在架构中的位置：

```
        Clerk ──▶ KvServer ──▶ Raft（共识 + 日志）
                       ▲              │
                       │       ApplyMsg（已提交的全序日志）
                       │              ▼
                       └──── 状态机：SkipList<K,V>
```

**Raft 只知道"日志条目是一个 `string`"，完全不知道这个 `string` 最终被塞进了跳表。** 这就是接口的可替换性所在——把 `SkipList` 换成 `std::map`、RocksDB、LevelDB 都不影响共识层。

**⚠️ 但这个文件不是本项目原创的。** 文件头保留了原作者信息：

```cpp
/* ************************************************************************
> File Name:     skiplist.h
> Author:        程序员Carl
> 微信公众号:    代码随想录
> Created Time:  Sun Dec  2 19:04:26 2018
> ************************************************************************/
```

它来自「代码随想录」的跳表教程，本项目在其基础上改造成了快照友好的版本（`dump_file` / `load_file` 返回/接收字符串，而非直接读写文件）。**这一点在统计"项目代码量"时值得注意**——和 `src/fiber`（monsoon 协程库）一样属于借用代码，而它们合计占了 `src/` 手写代码的 40%+。

---

## 2. 文件清单

| 文件 | 行数 | 说明 |
|---|---:|---|
| `include/skipList.h` | 479 | 全部实现（仅头文件，模板） |
| `CMakeLists.txt` | **0** | **空文件** |
| **合计** | **479** | |

**空 CMakeLists 不是疏漏，而是必需的**——`src/CMakeLists.txt` 里有 `add_subdirectory(skipList)`，如果目录下没有 `CMakeLists.txt`，CMake 会报错。由于跳表是纯模板头文件、没有 `.cpp` 需要编译，这个文件只需要存在即可。

**顺带纠正 CLAUDE.md 的一处描述**：

> 每个 `src/*` 模块都有自己的 `CMakeLists.txt`，把源文件导出到 `src_<模块名>` 这个 cache 变量

对 `skipList` 不成立——它**没有**导出 `src_skipList`（空文件无从导出）。根 `CMakeLists.txt` 里聚合库时也**确实没有引用它**：

```cmake
add_library(skip_list_on_raft STATIC
    ${src_rpc} ${src_fiber} ${rpc_example} ${raftsource} ${src_raftCore} ${src_raftRpcPro})
                                    # ↑ 没有 ${src_skipList}
```

能工作纯粹因为它是 header-only：`KvServer` 通过 `#include "skipList.h"` 直接拿到实现。

---

## 3. 跳表的数据结构

### 3.1 Node

```cpp
template <typename K, typename V>
class Node {
 public:
  Node(K k, V v, int);        // level 参数决定 forward 数组的长度
  K get_key() const;
  V get_value() const;
  void set_value(V);

  Node<K, V> **forward;       // ← 指向各层后继节点的指针数组
  int node_level;             // ← 该节点的层数

 private:
  K key;
  V value;
};
```

**`forward` 数组是跳表的全部精髓**：

```cpp
this->forward = new Node<K, V>*[level + 1];              // index 0..level
memset(this->forward, 0, sizeof(Node<K,V>*) * (level + 1));   // 全部置 NULL
```

一个层数为 `level` 的节点，有 `level + 1` 个前向指针，`forward[0]` 是第 0 层（最底层，含全部元素），`forward[i]` 是第 i 层的后继。

**`forward` 和 `node_level` 是 public，而 `key` / `value` 是 private** —— 封装不一致。外部可以直接改 `forward` 指针而绕过任何检查。当然，跳表的查找/插入算法本来就需要直接操作 `forward`，所以这个设计有其现实原因，但更干净的做法是让 `SkipList` 做 `friend`。

### 3.2 层级结构

```
        header
level 4   ─────────────────────────────────▶ 100
level 3   ─────▶ 10 ──────────────────────▶ 100
level 2   ─────▶ 10 ─────▶ 30 ────────────▶ 100
level 1   ─────▶ 10 ─────▶ 30 ────▶ 70 ───▶ 100
level 0   ──▶ 4 ▶ 10 ─9▶ 30 ▶40▶50▶60▶70 ─▶ 100
```

- **`_header`** 是一个层数为 `_max_level` 的哨兵节点，`key`/`value` 用默认构造值（`K()` / `V()`），不存数据。
- **`_skip_list_level`** 是当前跳表实际使用的最高层，随插入/删除动态变化。
- **`_max_level`** 是上限，`KvServer` 传的是 **6**（`kvServer.cpp:377`：`m_skipList(6)`）。

构造（`skipList.h:435`）：

```cpp
SkipList<K, V>::SkipList(int max_level) {
  this->_max_level = max_level;
  this->_skip_list_level = 0;
  this->_element_count = 0;
  K k; V v;
  this->_header = new Node<K, V>(k, v, _max_level);   // header 直接给最高层
}
```

### 3.3 随机层数

```cpp
template <typename K, typename V>
int SkipList<K, V>::get_random_level() {
  int k = 1;
  while (rand() % 2) k++;                       // 连续掷硬币，正面就升一层
  k = (k < _max_level) ? k : _max_level;
  return k;
}
```

**期望层数**：连续 `i` 次正面的概率是 `(1/2)^i`，所以第 i 层大约有 `n / 2^i` 个节点。这就是跳表 `O(log n)` 查找复杂度的来源——每层跳过约一半元素。

**注意 `k` 从 1 开始**，所以**最小层数是 1**（不是 0）。这意味着每个节点在 0 层和 1 层都有指针。功能上没问题（层 1 会包含全部元素），但比"最小层数 0"多用一个指针的空间。

**`rand()` 没有 `srand()` 播种** —— 和 `Clerk::Uuid()` 是同一个问题（raftClerk 说明疑点 2）。不过这里后果轻微：层数分布依然近似均匀，只是每次进程启动的序列相同。**主要风险是 `rand()` 不是线程安全的**（见疑点 5）。

---

## 4. 三种核心操作

三个操作共享同一套"自上而下逐层推进"的查找骨架：

```cpp
for (int i = _skip_list_level; i >= 0; i--) {
    while (current->forward[i] != NULL && current->forward[i]->get_key() < key)
        current = current->forward[i];
    update[i] = current;              // 记录每层"该在哪里插入/删除"
}
current = current->forward[0];        // 降到第 0 层，落到目标位置
```

### `search_element`（`skipList.h:402`）

只做查找，**不用 `update` 数组**（因为不修改结构）：

```cpp
bool SkipList<K, V>::search_element(K key, V &value) {
  std::cout << "search_element-----------------" << std::endl;   // ← 无条件打印
  Node<K, V> *current = _header;
  for (int i = _skip_list_level; i >= 0; i--)
    while (current->forward[i] && current->forward[i]->get_key() < key)
      current = current->forward[i];
  current = current->forward[0];
  if (current and current->get_key() == key) {
    value = current->get_value();
    std::cout << "Found key: " << key << ", value: " << current->get_value() << std::endl;
    return true;
  }
  std::cout << "Not Found Key:" << key << std::endl;
  return false;
}
```

**返回值通过引用参数 `V& value` 输出**，函数返回 bool 表示是否存在。这个签名和 `std::map::find` 的迭代器风格不同，是 C 风格的输出参数。

**三处 `std::cout` 是无条件的**——不受 `Debug` 开关控制，见[疑点 4](#8-已知疑点)。

### `insert_element`（`skipList.h:174`）

```cpp
int SkipList<K, V>::insert_element(const K key, const V value) {
  _mtx.lock();
  Node<K, V> *current = this->_header;
  Node<K, V> *update[_max_level + 1];
  memset(update, 0, sizeof(Node<K,V>*) * (_max_level + 1));

  // ① 逐层查找，记录 update[]
  for (int i = _skip_list_level; i >= 0; i--) {
    while (current->forward[i] != NULL && current->forward[i]->get_key() < key)
      current = current->forward[i];
    update[i] = current;
  }
  current = current->forward[0];

  // ② key 已存在 → 直接返回 1，【不更新 value】
  if (current != NULL && current->get_key() == key) {
    std::cout << "key: " << key << ", exists" << std::endl;
    _mtx.unlock();
    return 1;
  }

  // ③ 插入新节点
  if (current == NULL || current->get_key() != key) {
    int random_level = get_random_level();
    if (random_level > _skip_list_level) {          // 新节点比当前最高层还高
      for (int i = _skip_list_level + 1; i < random_level + 1; i++)
        update[i] = _header;                        // 高出部分从 header 出发
      _skip_list_level = random_level;
    }
    Node<K, V> *inserted_node = create_node(key, value, random_level);
    for (int i = 0; i <= random_level; i++) {       // 逐层接指针
      inserted_node->forward[i] = update[i]->forward[i];
      update[i]->forward[i] = inserted_node;
    }
    _element_count++;
  }
  _mtx.unlock();
  return 0;
}
```

**返回值语义**：`1` = key 已存在（什么都没做），`0` = 插入成功。**这不是错误码，是两个不同结果的区分**。

**⚠️ 关键行为：key 已存在时不更新 value。** 这直接导致了[疑点 2](#8-已知疑点)（快照恢复时旧值遮蔽新值）。

**注意 ② 里的 `_mtx.unlock()` 有两条退出路径**（正常返回和提前返回），都是手写 unlock——**没有用 RAII**。如果在中间的 `std::cout` 抛异常（理论上不会），锁就泄漏了。整个文件都是这个风格。

### `delete_element`（`skipList.h:331`）

```cpp
void SkipList<K, V>::delete_element(K key) {
  _mtx.lock();
  Node<K, V> *current = this->_header;
  Node<K, V> *update[_max_level + 1];
  // ... 同样的逐层查找，填 update[] ...

  current = current->forward[0];
  if (current != NULL && current->get_key() == key) {
    for (int i = 0; i <= _skip_list_level; i++) {
      if (update[i]->forward[i] != current) break;      // 第 i 层没连到它，更高层也没有
      update[i]->forward[i] = current->forward[i];      // 摘链
    }
    // 如果最高层空了，就降层
    while (_skip_list_level > 0 && _header->forward[_skip_list_level] == 0)
      _skip_list_level--;
    std::cout << "Successfully deleted key " << key << std::endl;
    delete current;
    _element_count--;
  }
  _mtx.unlock();
}
```

**`if (update[i]->forward[i] != current) break;` 是必要的**：目标节点不一定每层都存在。一旦某层没连到它，更高层更不会连到，可以直接跳出。

**没有返回值**——key 不存在时静默无事发生。

---

## 5. `insert_set_element`：唯一的写入口

```cpp
/**
 * \brief 作用与 insert_element 相同类似，
 * insert_element 是插入新元素，
 * insert_set_element 是插入元素，如果元素存在则改变其值
 */
template <typename K, typename V>
void SkipList<K, V>::insert_set_element(K &key, V &value) {
  V oldValue;
  if (search_element(key, oldValue)) {
    delete_element(key);          // ① 先删
  }
  insert_element(key, value);     // ② 再插
}
```

**这是 `KvServer` 唯一使用的写接口**（`kvServer.cpp:26` 和 `:68`，分别对应 Append 和 Put）。

它用"**先删后插**"来实现"更新"，因为 `insert_element` 遇到已存在的 key 会直接返回不更新。

**这样做有三个后果**：

1. **性能损失**：一次更新要 `search`（O(log n)）+ `delete`（O(log n)）+ `insert`（O(log n)），而真正需要的只是改一个 `value` 字段——O(1)。`Node::set_value()` 明明存在，却没被用上。
2. **节点层数会变**：新插入的节点重新掷骰子决定层数，与原来不同。功能上无害，但结构在无谓地变动。
3. **非原子**：`search` / `delete` / `insert` **各自独立加解锁**，中间有窗口。见[疑点 5](#8-已知疑点)。

**正确的实现**应该是查找后直接 `set_value`：

```cpp
Node<K,V>* n = find_node(key);
if (n) n->set_value(value); else insert_element(key, value);
```

**这个函数的存在本身就是 `insert_element` 语义不够好的补偿**——`insert` 本该就是 upsert，或者提供独立的 `update`。这是 API 设计问题，不是实现问题。

---

## 6. 快照接口

这是本项目对原始跳表的**主要改造点**。原本的版本直接读写 `store/dumpFile` 文件（`#define STORE_FILE "store/dumpFile"` 还留在第 22 行），改成了返回/接收字符串，以便嵌入 Raft 的快照机制。

### `dump_file` —— 序列化整个跳表

```cpp
template <typename K, typename V>
std::string SkipList<K, V>::dump_file() {
  Node<K, V> *node = this->_header->forward[0];     // 从第 0 层开始，遍历全部元素
  SkipListDump<K, V> dumper;
  while (node != nullptr) {
    dumper.insert(*node);
    node = node->forward[0];
  }
  std::stringstream ss;
  boost::archive::text_oarchive oa(ss);
  oa << dumper;
  return ss.str();
}
```

**只遍历第 0 层**——因为第 0 层包含全部元素，跳表的层级信息（随机层数）**不需要保存**，恢复时重新掷骰子即可。

**`SkipListDump` 是关键的设计**：

```cpp
template <typename K, typename V>
class SkipListDump {
 public:
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar &keyDumpVt_;
    ar &valDumpVt_;
  }
  std::vector<K> keyDumpVt_;
  std::vector<V> valDumpVt_;

 public:
  void insert(const Node<K, V> &node);      // 分别 push key 和 value
};
```

**为什么需要这个中间类？** 因为 boost 序列化**不能直接序列化 `Node`**——`Node` 里有裸指针 `forward` 数组，序列化指针是没有意义的。所以用一个扁平的"两个平行 vector"来表达整个跳表。

**这个设计是对的**，但下面的恢复代码没有用它。

### `load_file` —— 反序列化（**有致命 bug**）

```cpp
template <typename K, typename V>
void SkipList<K, V>::load_file(const std::string &dumpStr) {
  if (dumpStr.empty()) return;
  SkipListDump<K, V> dumper;
  std::stringstream iss(dumpStr);
  boost::archive::text_iarchive ia(iss);
  ia >> dumper;

  for (int i = 0; i < dumper.keyDumpVt_.size(); ++i) {
    insert_element(dumper.keyDumpVt_[i], dumper.keyDumpVt_[i]);
    //                                   ^^^^^^^^^^^^^^^^^^^^ 两个参数都是 key！
    //              dumper.valDumpVt_[i] 从未被使用
  }
}
```

**第 299 行把 key 当成了 value。** `valDumpVt_` 在 `dump_file` 时被正确填充，在 `load_file` 时被完全忽略。详见[疑点 1](#8-已知疑点)。

**另外 `load_file` 不清理已有数据**——它直接往现有跳表里插，没有 `clear()` 也没有重建。这在"恢复"场景下是错的，见[疑点 2](#8-已知疑点)。

---

## 7. 与 KvServer 的耦合

### 调用点

| KvServer 位置 | 调用的跳表方法 | 场景 |
|---|---|---|
| `ExecuteAppendOpOnKVDB:26` | `insert_set_element` | 应用 Append 命令 |
| `ExecutePutOpOnKVDB:68` | `insert_set_element` | 应用 Put 命令 |
| `ExecuteGetOpOnKVDB:45` | `search_element` | 应用 Get 命令（读） |
| `DprintfKVDB:16` | `display_list` | 调试打印（每条命令后都调用） |
| `getSnapshotData:115` | `dump_file` | 打快照 |
| `parseFromString:127` | `load_file` | 恢复快照 |

### 快照的完整闭环

```
【打快照】  Raft state 超过阈值
   KvServer::IfNeedToSendSnapShotCommand
     └─ MakeSnapShot
          └─ getSnapshotData()                              kvServer.h:114
               ├─ m_serializedKVData = m_skipList.dump_file()   ← 跳表 → 字符串
               └─ boost 序列化 { m_serializedKVData, m_lastRequestId }
          └─ m_raftNode->Snapshot(raftIndex, snapshot)
               └─ Persister::Save()   ← 落盘

【恢复快照】  节点重启 或 收到 leader 的快照
     └─ KvServer::ReadSnapShotToInstall(snapshot)           kvServer.cpp:303
          └─ parseFromString(str)                           kvServer.h:123
               ├─ boost 反序列化 → m_serializedKVData, m_lastRequestId
               └─ m_skipList.load_file(m_serializedKVData)  ← 字符串 → 跳表 【BUG 在此】
```

**注意快照里包含两部分**：跳表数据**和** `m_lastRequestId` 去重表。两者缺一不可——只恢复数据不恢复去重表的话，重启后重复请求会被再次执行（raftCore 说明第 13 节）。

**跳表数据是"被嵌套序列化"的**：

```
boost text archive          ← 外层（KvServer::getSnapshotData）
  └─ m_serializedKVData: string
       └─ 内容是另一份 boost text archive   ← 内层（SkipList::dump_file）
            └─ SkipListDump<K,V> { keyDumpVt_, valDumpVt_ }
```

这是"把跳表当成一个不透明的字符串塞进 KvServer 的快照"的代价——**两层 boost 文本归档嵌套**。能用，但每个字符都被转义了一层，体积膨胀。

**这个设计的好处**是解耦：`KvServer` 不需要知道跳表的内部结构，`SkipList` 也不需要知道快照的存在。CLAUDE.md 里那句"理论上可替换为任意 KV 存储"正是靠这个边界成立的。

---

## 8. 已知疑点

> ⚠️ 均为**静态阅读**发现，未运行验证。疑点 1、2 是数据正确性问题。
>
> **更新（已修复）**：疑点 1（`load_file` 把 key 当 value）与疑点 2（`load_file` 不清空已有数据）
> 已经确认并修复，改动见 `benchmark/README.md` §8.5 的第 6、7 条。
> 完整缺陷清单与实测证据见 [benchmark/README.md](../../benchmark/README.md#85-压测中发现并已修复的缺陷)。

### 1. 🔴 `load_file` 把 key 当 value —— 恢复后所有 value 变成 key

`skipList.h:298-300`：

```cpp
for (int i = 0; i < dumper.keyDumpVt_.size(); ++i) {
  insert_element(dumper.keyDumpVt_[i], dumper.keyDumpVt_[i]);
  //                                └── 应该是 dumper.valDumpVt_[i]
}
```

**后果**：任何一次快照恢复之后，`SkipList` 中每个 key 对应的 value 都变成 **key 自身**。

举个例子：

```cpp
// 快照前
Put("name", "alice")  →  SkipList: { "name" → "alice" }
                        dump_file() → SkipListDump{ keyVt:["name"], valVt:["alice"] }

// 恢复后
load_file()           →  SkipList: { "name" → "name" }     ← 数据损坏
```

**触发路径**（两条，都是正常运行时路径）：

1. **节点重启**：`KvServer` 构造函数 `persister->ReadSnapshot()` → `ReadSnapShotToInstall` → `parseFromString` → `load_file`。
2. **follower 接收 leader 的快照**：`GetSnapShotFromRaft` → `ReadSnapShotToInstall` → 同上。

**这条路径在 `m_maxRaftState = 500` 的配置下几乎必然走到**（raftCore 说明第 13 节：快照阈值是 50 字节，几乎每 apply 一条就触发一次快照）。

**为什么可能没被察觉**：如果测试用例只做 `Put(k, k)` 这种 key 等于 value 的写入，或者只验证"key 存在"不验证 value，bug 不会暴露。**验证方法**：写入 `Put("a", "b")`，等快照触发（或直接重启节点），再 `Get("a")`，观察返回的是 `"b"` 还是 `"a"`。

**修复**：

```cpp
insert_element(dumper.keyDumpVt_[i], dumper.valDumpVt_[i]);
```

### 2. 🔴 `load_file` 不清理已有数据 —— 快照是"合并"而非"替换"

`load_file` 直接往跳表里插，没有清空。在 `KvServer` 构造函数里跳表是空的，所以首次恢复没问题。但 **`GetSnapShotFromRaft` 路径上跳表已经装了数据**：

```
节点运行一段时间，跳表里有 { A→1, B→2 }
leader 发来快照：{ A→9, C→3 }
load_file 执行后 → { A→1, B→2, C→3 }    ← A 保留了旧值 1，而非快照中的 9
```

**两层 bug 叠加**：

- 对于**已存在的 key**：`insert_element` 遇到已存在的 key 会**直接返回、不更新**（`skipList.h:195-199`）→ 旧值保留，**快照中的新值被丢弃**。
- 对于**新的 key**：走[疑点 1](#8-已知疑点) 的路径，value 被写成 key。

**结果是快照恢复后的状态既不是旧的也不是新的**。

**正确的实现**应该在加载前清空：

```cpp
void SkipList<K,V>::load_file(const std::string& dumpStr) {
  if (dumpStr.empty()) return;
  // 清空现有数据（递归 delete 所有节点，或直接 delete _header 重建）
  ...
  for (...) insert_element(dumper.keyDumpVt_[i], dumper.valDumpVt_[i]);
}
```

### 3. `search_element` 与写操作之间的竞态

`search_element`（`skipList.h:402`）、`display_list`（`:232`）、`size`（`:305`）、`dump_file`（`:249`）**都不加锁**，而 `insert_element` / `delete_element` 加锁。

**锁的粒度不一致意味着跳表的 `_mtx` 并不能提供真正的线程安全**。文件里第 246 行作者的注释也承认了这点：

```cpp
// todo 对dump 和 load 后面可能要考虑加锁的问题
```

**在当前项目里这条暂时不致命**，因为 `KvServer` 自己有个 `m_mtx` 包在外面：

```cpp
void KvServer::ExecuteGetOpOnKVDB(Op op, std::string* value, bool* exist) {
  m_mtx.lock();
  if (m_skipList.search_element(op.Key, *value)) { ... }
  m_mtx.unlock();
}
```

所有跳表访问都在 `KvServer::m_mtx` 保护下，**外部串行化了**。但这是"用户代码凑巧把库的缺陷盖住了"——`SkipList` 对外承诺线程安全（有 `_mtx` 成员），实际不成立，换一个使用场景就会出问题。

### 4. 无条件 `std::cout` —— 不受 `Debug` 开关控制

`search_element` 里三处、`insert_element` 里两处、`delete_element` 里一处 `std::cout`，**都是无条件的**。

而项目的日志规范是（CLAUDE.md）：

> `Debug` 控制 `DPrintf` 输出

`DPrintf` 有 `if (Debug)` 保护，但这些 `std::cout` 绕过了它。**后果**：

- 每次 `Get` 都打印 `"search_element-----------------"` + `"Found key: ..."`；
- 每次写入都打印 `"Successfully inserted key:..."`；
- **无法通过任何配置关闭**，只能改源码。

在压测（`callerMain`）场景下，控制台输出会成为主要瓶颈——IO 同步写远比跳表操作本身慢。

### 5. `insert_set_element` 非原子 + `rand()` 非线程安全

```cpp
void SkipList<K,V>::insert_set_element(K& key, V& value) {
  V oldValue;
  if (search_element(key, oldValue)) {    // ← 不加锁
    delete_element(key);                  // ← 独立加解锁
  }
  insert_element(key, value);             // ← 独立加解锁
}
```

**三步各自独立加锁，整体不是原子操作**。两个线程同时 `insert_set_element` 同一个 key，可能出现交叉：

```
线程A: search(key) → 存在
线程B: search(key) → 存在
线程A: delete(key) → 删掉了
线程B: delete(key) → 什么都没删（已不存在）
线程A: insert(key, vA)
线程B: insert(key, vB) → key 已存在，直接返回，vB 丢失
```

结果：A 的值胜出，但 B 的调用返回"成功"。**在 `KvServer` 里这同样被外层 `m_mtx` 盖住了**，但库本身的保证不成立。

**另外 `get_random_level()` 里的 `rand()` 不是线程安全的**。glibc 的 `rand()` 用全局状态且无锁，多线程并发调用会读到撕裂的状态。实践后果轻微（层数分布略偏），但属于未定义行为。

### 6. `display_list` 在每条命令后被调用 —— O(n) 打印

```cpp
// kvServer.cpp:7
void KvServer::DprintfKVDB() {
  if (!Debug) return;
  std::lock_guard<std::mutex> lg(m_mtx);
  DEFER { m_skipList.display_list(); };     // 打印整个跳表所有层
}
```

而 `DprintfKVDB()` 在 `ExecuteAppendOpOnKVDB` / `ExecuteGetOpOnKVDB` / `ExecutePutOpOnKVDB` 的**结尾都被调用一次**。

`display_list` 遍历所有层、打印所有元素——**O(n)**。于是每应用一条命令就是 O(n) 的输出，总复杂度 **O(n²)**。

`Debug = true` 时（`config.h:8`，默认就是 true），跑几千条命令就会淹没在输出里。**这是压测时最先撞到的性能墙**，比 Raft 的算法开销严重得多。

### 7. 死代码与残留

- **`_file_writer` / `_file_reader`**：两个 `ofstream`/`ifstream` 成员，所有使用点都被注释掉了（`dump_file` / `load_file` 改成了字符串版本）。析构函数里还在检查 `is_open()`。
- **`#define STORE_FILE "store/dumpFile"`**（`:22`）：文件路径宏，已无用。
- **`get_key_value_from_string` / `is_valid_string`**（`:310`、`:319`）：为解析文件行而写，现在没有任何调用点。
- **`static std::string delimiter = ":"`**（`:24`）：配合上面两个函数使用，同样已无用。它是头文件里的 `static`，每个翻译单元一份副本。

**这些都是"从文件版改造成字符串版"时没清理干净的残留。**

### 8. `clear()` 递归 —— 深表会栈溢出

```cpp
template <typename K, typename V>
void SkipList<K,V>::clear(Node<K,V>* cur) {
  if (cur->forward[0] != nullptr) clear(cur->forward[0]);   // 递归
  delete cur;
}
```

析构时递归遍历第 0 层的整个链条。**元素数达到十万级时，递归深度等于元素数**，默认 8MB 栈（每帧若干字节）大约能撑到几十万——**百万级元素时栈溢出**。

应该改成迭代：

```cpp
Node<K,V>* cur = _header->forward[0];
while (cur) { Node<K,V>* next = cur->forward[0]; delete cur; cur = next; }
```

### 9. 返回值语义不统一

| 方法 | 返回类型 | 语义 |
|---|---|---|
| `insert_element` | `int` | `1` = 已存在，`0` = 插入成功 |
| `insert_set_element` | `void` | 无反馈 |
| `search_element` | `bool` + 输出参数 | `true` = 找到 |
| `delete_element` | `void` | 无反馈（key 不存在时静默） |
| `size` | `int` | 元素数 |

`int` 返回 0/1 表示"两种结果"而非"成功/失败"，容易被误用为错误码。`delete_element` 不返回是否删除成功，调用方无法判断。

### 10. `insert_element` 的参数按值传递

```cpp
int insert_element(K key, V value);          // 都是值传递
```

对 `std::string` 而言每次调用都有两份拷贝。改用 `const K&` / `const V&` 即可。当前项目里 `K`/`V` 都是 `std::string`，且写入频率不高，实际影响有限。

---

## 9. 作为"可替换状态机"的评估

CLAUDE.md 说它"理论上可替换为任意 KV 存储"。这个说法**对，但有个前提**。

### 接口契约

一个替换实现需要满足：

| 要求 | 说明 |
|---|---|
| `insert_set_element(K&, V&)` | 写入/更新 |
| `search_element(K, V&)` | 读取，bool + 输出参数 |
| `dump_file() → std::string` | 整个状态序列化为字符串 |
| `load_file(const std::string&)` | 从字符串恢复 |

**注意 `KvServer` 只用了这四个方法**（加上 `display_list` 做调试）。`delete_element` / `size` / `insert_element` 等虽然存在，但状态机层没用到。

### 替换的难点不在接口，在序列化

**`dump_file` / `load_file` 的字符串格式是 `SkipList` 私有的**——里面是 `SkipListDump` 的 boost text archive。换成 `std::map` 就得自己实现一套等价的序列化，而且**格式必须能向后兼容**（旧快照文件还得能读）。

这意味着"可替换"实际要求是：新实现要能理解**旧跳表的快照格式**，否则升级即丢数据。

**更干净的设计**是让 `KvServer` 负责序列化，把状态机抽象成一个 `put(key,value)` / `get(key)` / `iterate()` 的接口：

```
KvServer 持有快照格式的所有权
   └─ 状态机只提供遍历能力
        └─ 具体是 SkipList 还是 RocksDB 都无所谓
```

当前设计把格式所有权放在了 `SkipList` 里，**耦合比看起来更紧**。

### 性能评估

| 维度 | 跳表 | 对比 |
|---|---|---|
| 查找 | O(log n) 期望 | 与平衡树同阶 |
| 插入 | O(log n) 期望 | 同上，但常数更小（无旋转） |
| 范围查询 | 天然支持（第 0 层是链表） | 树的优势区域 |
| 并发 | **本实现未真正支持**（疑点 3、5） | 跳表本身易改造成无锁 |
| 持久化 | 全量快照 | 没有 WAL、没有增量 |
| 内存 | 期望每元素 2 个指针（1/(1-1/2)） | 比 B 树占用大 |

**对于这个教学项目，跳表够用且合适**——插入/删除不需要旋转，代码比红黑树短得多（479 行含注释），且第 0 层的链表天然支持快照时的顺序遍历。

**真正的短板是持久化**：`dump_file` 是全量序列化，没有增量。配上 `m_maxRaftState = 500` 的配置，就是每 apply 一条命令做一次全量序列化 + 一次全量文件写入。

---

## 10. 跳表本身值得学什么

抛开这个项目的具体实现，跳表作为数据结构有几点值得带走：

1. **"用随机化换平衡"**（`get_random_level`）。红黑树/AVL 用旋转维持严格平衡，跳表用掷硬币维持**概率平衡**。代价是 O(log n) 是**期望值**而非最坏值，收益是实现简单一个数量级。

2. **`forward` 数组 = 多层的链表**。每个节点持有若干个"快速通道"指针，查找时从最高层开始、逐层下降。这个"以空间换时间"的思路和 B 树的"以扇出减高度"是同一类。

3. **`update[]` 数组的技巧**。插入/删除时要修改所有层的前驱指针，`update[i]` 在查找过程中顺手记下"第 i 层该改谁"。**一次查找同时完成定位和"路径记录"**，这是跳表实现优雅的地方。

4. **层数上限 `_max_level` 是性能旋钮**。理论上 `log₂(n)` 层最优；给 6 层的话大约能高效支撑 `2^6 = 64` 个元素，再多就退化了。**`KvServer` 传的 6 对于大 KV 数据是偏小的**——这是一个容易被忽略的参数。

   > 实际上退化的影响有限：即使层数不够，第 0 层仍是完整链表，查找最坏退化成 O(n)，但平均仍远好于线性。`_max_level` 影响的是"最高层能跳多远"。

5. **跳表是 Redis / LevelDB 的选择**。Redis 的有序集合用跳表而不是红黑树，理由正是"实现简单 + 范围查询友好 + 并发改造容易"。

---

## 附：这个模块值得学什么

**从项目角度**：它是"状态机"这个概念最直接的体现——`Raft` 产出一个 `string` 序列，`KvServer` 把它变成 KV 操作，`SkipList` 存下来。三层各不知道上层的细节。

**从代码角度**：它是**一个"改造未完成"的典型案例**。从文件版改成字符串版时留下了死代码（`_file_writer`、`STORE_FILE`、`get_key_value_from_string`），并且改造过程中引入了一个关键 bug（`load_file` 的 value = key）。**读它最大的收获是学会怀疑"序列化/反序列化"这一对函数是否真的对称**——[疑点 1](#8-已知疑点) 就是这么漏出来的：`dump` 写两个 vector，`load` 只读了一个。

**读法建议**：479 行可以完整读完。重点看 `insert_element` 的 `update[]` 技巧（`skipList.h:174-228`），然后**逐行对比 `dump_file` 和 `load_file`**——这一对函数是全文最值得警惕的地方。

**如果只做一件事**：修复[疑点 1](#8-已知疑点)（一行改动），然后写个测试验证 `Put(k,v)` → 快照 → 恢复 → `Get(k)` 返回的是 `v` 而不是 `k`。

---

## 相关文档

- [raftCore 说明](../raftCore/raftCore说明.md) —— 快照机制的完整上下文
- [raftClerk 说明](../raftClerk/raftClerk说明.md) —— 去重表 `m_lastRequestId`，与跳表数据一起进快照
- [rpc 说明](../rpc/rpc说明.md) —— 传输层
- [raftRpcPro 说明](../raftRpcPro/raftRpcPro说明.md) —— 协议定义
- [CLAUDE.md](../../CLAUDE.md) —— 构建、运行与工作目录的坑
