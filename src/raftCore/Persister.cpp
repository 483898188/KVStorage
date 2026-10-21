//
// Created by swx on 23-5-30.
//
#include "Persister.h"
#include <sstream>
#include "util.h"

// todo:会涉及反复打开文件的操作，没有考虑如果文件出现问题会怎么办？？
void Persister::Save(const std::string raftstate, const std::string snapshot) {
  std::lock_guard<std::mutex> lg(m_mtx);
  clearRaftStateAndSnapshot();
  // 将raftstate和snapshot写入本地文件
  m_raftStateOutStream << raftstate;
  m_snapshotOutStream << snapshot;
  // 必须 flush：ofstream 是带缓冲的，不 flush 的话数据停在用户态缓冲区里，
  // 进程被 SIGKILL 时直接丢失。而 persist() 的语义是"在对外响应前必须落盘"
  // （Raft 要求 votedFor/currentTerm 先持久化再回复投票），缓冲不刷 = 耐久性不成立。
  m_raftStateOutStream.flush();
  m_snapshotOutStream.flush();
}

std::string Persister::ReadSnapshot() {
  std::lock_guard<std::mutex> lg(m_mtx);
  if (m_snapshotOutStream.is_open()) {
    m_snapshotOutStream.close();
  }

  DEFER {
    // 必须以 app 模式重开。ofstream::open 的默认 mode 是 ios::out，
    // 对【已存在】的文件会截断 —— 原实现注释写"默认是追加"，那是错的。
    // 结果是：每次读快照都会把快照文件清零，下一个启动周期自然读不到任何东西。
    m_snapshotOutStream.open(m_snapshotFileName, std::ios::out | std::ios::app);
  };

  std::ifstream ifs(m_snapshotFileName, std::ios::in | std::ios::binary);
  if (!ifs.is_open()) {
    return "";
  }
  // 必须整体读取。原实现是 `ifs >> snapshot`，而 operator>> 遇到第一个空白字符就停，
  // 快照是 boost 文本归档、内容里全是空格换行 —— 那样只能读到第一个词。
  std::stringstream ss;
  ss << ifs.rdbuf();
  ifs.close();
  return ss.str();
}

void Persister::SaveRaftState(const std::string &data) {
  std::lock_guard<std::mutex> lg(m_mtx);
  // 将raftstate和snapshot写入本地文件
  clearRaftState();
  m_raftStateOutStream << data;
  // 见 Save() 的说明：不 flush 的话崩溃会丢掉最后一次 persist。
  // 注意这里【不能】用 fsync 之外更弱的手段糊弄 —— flush 至少保证数据离开了
  // 用户态缓冲区；要抗断电还需要 fsync，本项目从简。
  m_raftStateOutStream.flush();
  m_raftStateSize += data.size();
}

long long Persister::RaftStateSize() {
  std::lock_guard<std::mutex> lg(m_mtx);

  return m_raftStateSize;
}

std::string Persister::ReadRaftState() {
  std::lock_guard<std::mutex> lg(m_mtx);

  std::ifstream ifs(m_raftStateFileName, std::ios::in | std::ios::binary);
  if (!ifs.is_open()) {
    return "";
  }
  // 同 ReadSnapshot：不能用 `ifs >> str`，boost 文本归档里有空白会被截断。
  std::stringstream ss;
  ss << ifs.rdbuf();
  ifs.close();
  return ss.str();
}

Persister::Persister(const int me)
    : m_raftStateFileName("raftstatePersist" + std::to_string(me) + ".txt"),
      m_snapshotFileName("snapshotPersist" + std::to_string(me) + ".txt"),
      m_raftStateSize(0) {
  /**
   * 只确保文件存在，【不要】清空已有内容。
   *
   * 原实现用 std::ios::out | std::ios::trunc 打开两个文件 —— 那会在每次进程启动时
   * 把上一次落盘的 raftstate / snapshot 全部抹掉，而 KvServer 构造里紧接着就要调
   * persister->ReadSnapshot() 去恢复状态，读到的必然是空。于是：
   *   - 崩溃恢复实际从未生效（benchmark/_persist_probe.sh 已实测证明）
   *   - Raft 论文 Figure 2 的持久化要求在实现上被架空，
   *     而"当选者必含所有已提交日志"这条安全性保证正依赖它
   *
   * 这里改成"不存在则创建"，已有内容原样保留。
   */
  bool fileOpenFlag = true;
  for (const std::string &fname : {m_raftStateFileName, m_snapshotFileName}) {
    // app 模式：文件不存在则创建；存在则【不截断】，指针停在末尾
    std::ofstream probe(fname, std::ios::out | std::ios::app);
    if (probe.is_open()) {
      probe.close();
    } else {
      fileOpenFlag = false;
    }
  }
  if (!fileOpenFlag) {
    DPrintf("[func-Persister::Persister] file open error");
  }
  /**
   * 绑定写流。
   * 必须用 app：ofstream::open 默认是 ios::out，对已存在的文件同样会截断。
   * 真正需要清空时由 clearRaftState() / clearSnapshot() 显式 trunc —— 它们只在
   * 覆盖写入前调用，不影响"启动时保留上次状态"。
   */
  m_raftStateOutStream.open(m_raftStateFileName, std::ios::out | std::ios::app);
  m_snapshotOutStream.open(m_snapshotFileName, std::ios::out | std::ios::app);

  // 已有内容要计入大小，否则 GetRaftStateSize() 从 0 起算，快照触发时机会失准
  {
    std::ifstream in(m_raftStateFileName, std::ios::in | std::ios::binary | std::ios::ate);
    if (in.is_open()) {
      m_raftStateSize = static_cast<long long>(in.tellg());
      in.close();
    }
  }
}

Persister::~Persister() {
  if (m_raftStateOutStream.is_open()) {
    m_raftStateOutStream.close();
  }
  if (m_snapshotOutStream.is_open()) {
    m_snapshotOutStream.close();
  }
}

void Persister::clearRaftState() {
  m_raftStateSize = 0;
  // 关闭文件流
  if (m_raftStateOutStream.is_open()) {
    m_raftStateOutStream.close();
  }
  // 重新打开文件流并清空文件内容
  m_raftStateOutStream.open(m_raftStateFileName, std::ios::out | std::ios::trunc);
}

void Persister::clearSnapshot() {
  if (m_snapshotOutStream.is_open()) {
    m_snapshotOutStream.close();
  }
  m_snapshotOutStream.open(m_snapshotFileName, std::ios::out | std::ios::trunc);
}

void Persister::clearRaftStateAndSnapshot() {
  clearRaftState();
  clearSnapshot();
}
