//
// Created by swx on 23-6-4.
//

#ifndef SKIP_LIST_ON_RAFT_CLERK_H
#define SKIP_LIST_ON_RAFT_CLERK_H
#include <arpa/inet.h>
#include <netinet/in.h>
#include <raftServerRpcUtil.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <cerrno>
#include <chrono>
#include <mutex>
#include <random>
#include <string>
#include <vector>
#include "kvServerRPC.pb.h"
#include "mprpcconfig.h"
class Clerk {
 private:
  std::vector<std::shared_ptr<raftServerRpcUtil>>
      m_servers;  //保存所有raft节点的fd //todo：全部初始化为-1，表示没有连接上
  std::string m_clientId;
  int m_requestId;
  int m_recentLeaderId;  //只是有可能是领导

  // 返回随机的 clientId。
  //
  // 原实现是 `to_string(rand())` 拼四次，但从未调用 srand()，于是【每个进程】拿到的
  // rand() 序列完全相同 —— 两个客户端进程连同一个集群时会得到同一个 clientId。
  // 服务端 m_lastRequestId[clientId] 里已存有第一个进程的进度，第二个进程的 RequestId
  // 一律 <= 该值，被 ifRequestDuplicate 判为重复而不执行，却仍然返回 OK —— 静默丢写。
  // （已由 benchmark/dedup_test.sh 实测复现：第二个进程 300/300 次写入全部丢失且回 OK）
  //
  // 这里改用 random_device 播种，并混入 pid 与单调时钟，保证跨进程不会重复；
  // 静态 + 加锁保证同进程内多线程也各拿各的。
  std::string Uuid() {
    static std::mutex mtx;
    static std::mt19937_64 rng{std::random_device{}() ^
                               (static_cast<uint64_t>(::getpid()) << 32) ^
                               static_cast<uint64_t>(
                                   std::chrono::steady_clock::now().time_since_epoch().count())};
    std::lock_guard<std::mutex> lg(mtx);
    return std::to_string(rng()) + std::to_string(rng());
  }

  //    MakeClerk  todo
  void PutAppend(std::string key, std::string value, std::string op);

 public:
  //对外暴露的三个功能和初始化
  void Init(std::string configFileName);
  std::string Get(std::string key);

  void Put(std::string key, std::string value);
  void Append(std::string key, std::string value);

 public:
  Clerk();
};

#endif  // SKIP_LIST_ON_RAFT_CLERK_H
