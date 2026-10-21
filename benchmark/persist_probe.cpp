// 验证 Persister 的"落盘 -> 重启 -> 恢复"是否真的能工作。
// 用法：编译进 /tmp 运行，不影响仓库。
#include <iostream>
#include <string>
#include "Persister.h"

int main() {
  const int node = 99;   // 用独立编号，避免撞上正在用的文件

  // 第一轮：模拟一个节点运行中写入了状态
  {
    Persister p(node);
    p.Save("<<RAFTSTATE-BLOB>>", "<<SNAPSHOT-BLOB>>");
    std::cout << "[写入轮] 已调用 Save(raftstate, snapshot)\n";
  }

  // 检查文件真的落盘了
  {
    std::cout << "[写入轮] raftstatePersist99.txt 大小: ";
    std::cout.flush();
    system("stat -c %s raftstatePersist99.txt 2>/dev/null || echo '不存在'");
    std::cout << "[写入轮] snapshotPersist99.txt 大小: ";
    std::cout.flush();
    system("stat -c %s snapshotPersist99.txt 2>/dev/null || echo '不存在'");
  }

  // 第二轮：模拟进程重启（重新构造 Persister 并读取）
  {
    Persister p(node);
    std::string rs = p.ReadRaftState();
    std::string ss = p.ReadSnapshot();
    std::cout << "[重启轮] ReadRaftState() -> \"" << rs << "\"  (长度 " << rs.size() << ")\n";
    std::cout << "[重启轮] ReadSnapshot()  -> \"" << ss << "\"  (长度 " << ss.size() << ")\n";
    if (rs.empty() && ss.empty()) {
      std::cout << "[结论] ❌ 重启后读不到任何持久化状态 —— 崩溃恢复实际未生效\n";
    } else {
      std::cout << "[结论] ✅ 读回了持久化状态\n";
    }
  }
  return 0;
}
