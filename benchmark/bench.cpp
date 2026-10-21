//
// benchmark/bench.cpp —— Raft KV 集群读写压测
//
// 设计要点（详见 benchmark/README.md）：
//   1. 每个线程一个 Clerk —— Clerk 完全没有锁（clerk.h:22-24），共享会读到撕裂的值
//   2. 每个线程独占一段 key 空间（k_<线程>_<序号>），避免并发写同一 key 导致校验失败
//   3. 去掉所有逐条 printf —— 否则测的是 stdout，不是 Raft
//   4. 写完后做全量读回校验 —— 验证"已确认的写入不丢"
//
// 注意：Clerk::Put 返回 void，失败对调用方不可见（内部无限重试 ErrWrongLeader）。
//       所以下面的 QPS 是"完成速率"，无法区分"快"和"重试了很多次才成功"。
//       如果集群根本没起来，程序会永久挂住（Clerk 无限重试 + mprpc 无超时），
//       所以这里带一个看门狗线程持续打印进度。
//
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <numeric>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "clerk.h"
#include "util.h"

namespace {

using Clock = std::chrono::steady_clock;

int percentileIndex(size_t n, double p) {
  if (n == 0) return 0;
  size_t idx = static_cast<size_t>(static_cast<double>(n) * p);
  if (idx >= n) idx = n - 1;
  return static_cast<int>(idx);
}

// 把延迟样本排序后打印分位数
void printLatency(const char* tag, std::vector<double>& v) {
  if (v.empty()) {
    printf("%-8s 无样本\n", tag);
    return;
  }
  std::sort(v.begin(), v.end());
  double avg = std::accumulate(v.begin(), v.end(), 0.0) / static_cast<double>(v.size());
  printf("%-8s n=%-8zu  avg=%7.2fms  P50=%7.2fms  P90=%7.2fms  P99=%7.2fms  max=%7.2fms\n", tag, v.size(), avg,
         v[percentileIndex(v.size(), 0.50)], v[percentileIndex(v.size(), 0.90)],
         v[percentileIndex(v.size(), 0.99)], v.back());
}

std::string keyOf(int t, int i) { return "k_" + std::to_string(t) + "_" + std::to_string(i); }

// 值里带前缀，这样"同一 key 被两次运行写入"时能分辨出到底写进去的是哪一次的值。
// 不带前缀（-p 缺省）时退化为原来的 v_<线程>_<序号>。
std::string g_valPrefix;
std::string valOf(int t, int i) {
  return g_valPrefix + "v_" + std::to_string(t) + "_" + std::to_string(i);
}

void usage(const char* prog) {
  printf(
      "用法: %s [选项]\n"
      "  -t <n>   客户端线程数        (默认 2，4 核机器建议不超过 4)\n"
      "  -n <n>   每线程操作数        (默认 2000)\n"
      "  -w <n>   每线程预热操作数    (默认 100，不计入统计)\n"
      "  -m <m>   模式: put|get|mix   (默认 put；mix = 4 写 1 读)\n"
      "  -f <f>   配置文件            (默认 test.conf，需在 bin/ 下运行)\n"
      "  -p <s>   值前缀              (默认空；用于区分同一 key 的多次运行)\n"
      "  -c       跳过写完后的读回校验\n"
      "  -h       显示帮助\n",
      prog);
}

}  // namespace

int main(int argc, char** argv) {
  int nThreads = 2;
  int nOps = 2000;
  int nWarmup = 100;
  bool doVerify = true;
  std::string mode = "put";
  std::string cfg = "test.conf";

  int opt;
  while ((opt = getopt(argc, argv, "t:n:w:m:f:p:ch")) != -1) {
    switch (opt) {
      case 't': nThreads = atoi(optarg); break;
      case 'n': nOps = atoi(optarg); break;
      case 'w': nWarmup = atoi(optarg); break;
      case 'm': mode = optarg; break;
      case 'f': cfg = optarg; break;
      case 'p': g_valPrefix = optarg; break;
      case 'c': doVerify = false; break;
      case 'h':
      default: usage(argv[0]); return opt == 'h' ? 0 : 1;
    }
  }
  if (nThreads <= 0 || nOps <= 0) { usage(argv[0]); return 1; }

  printf("=== RaftKV 压测 ===\n");
  printf("配置: threads=%d ops/thread=%d warmup=%d mode=%s cfg=%s verify=%s\n\n", nThreads, nOps, nWarmup,
         mode.c_str(), cfg.c_str(), doVerify ? "on" : "off");

  // ---------------- 预热（单线程，结果丢弃）----------------
  // 目的：让集群选完 leader、让 Clerk 定位到 leader，避免把冷启动成本算进正式统计
  if (nWarmup > 0 && mode != "get") {
    printf("[预热] %d 次写入…\n", nWarmup);
    Clerk warm;
    warm.Init(cfg);
    for (int i = 0; i < nWarmup; ++i) warm.Put(keyOf(0, i), valOf(0, i));
    printf("[预热] 完成\n\n");
  }

  // ---------------- 正式压测 ----------------
  std::atomic<long long> done{0};         // 已完成的操作数（注意：不等于成功数，见文件头注释）
  std::atomic<bool> finished{false};
  std::vector<double> latencies;          // 单位 ms
  std::mutex latMtx;
  latencies.reserve(static_cast<size_t>(nThreads) * static_cast<size_t>(nOps));

  // 看门狗：集群挂掉时 Clerk 会无限重试，没有这个线程你会以为只是慢
  std::thread watchdog([&] {
    int tick = 0;
    while (!finished.load()) {
      std::this_thread::sleep_for(std::chrono::seconds(5));
      if (finished.load()) break;
      printf("  … 已运行 %2ds，完成 %lld / %d 次操作\n", (++tick) * 5, done.load(), nThreads * nOps);
      fflush(stdout);
    }
  });

  printf("[压测] 开始…\n");
  auto t0 = Clock::now();
  std::vector<std::thread> pool;
  pool.reserve(static_cast<size_t>(nThreads));
  for (int t = 0; t < nThreads; ++t) {
    pool.emplace_back([&, t] {
      Clerk clerk;
      clerk.Init(cfg);                    // 每线程独立 Clerk —— 不可共享
      std::vector<double> local;
      local.reserve(static_cast<size_t>(nOps));

      for (int i = 0; i < nOps; ++i) {
        std::string k = keyOf(t, i);
        std::string v = valOf(t, i);

        auto s = Clock::now();
        if (mode == "put") {
          clerk.Put(k, v);
        } else if (mode == "get") {
          clerk.Get(k);
        } else {                          // mix: 4 写 1 读
          if (i % 5 == 4) {
            clerk.Get(k);
          } else {
            clerk.Put(k, v);
          }
        }
        auto e = Clock::now();

        local.push_back(std::chrono::duration<double, std::milli>(e - s).count());
        done.fetch_add(1);
      }

      std::lock_guard<std::mutex> lg(latMtx);
      latencies.insert(latencies.end(), local.begin(), local.end());
    });
  }
  for (auto& th : pool) th.join();
  auto t1 = Clock::now();
  finished.store(true);
  watchdog.join();

  double secs = std::chrono::duration<double>(t1 - t0).count();
  long long total = done.load();
  printf("\n[结果] 完成 %lld 次操作，耗时 %.2fs，吞吐 %.0f ops/s\n", total, secs, static_cast<double>(total) / secs);
  printLatency("[延迟]", latencies);
  fflush(stdout);

  // ---------------- 读回校验 ----------------
  // 校验"已确认的写入不丢"。这是 Raft 的核心安全性属性，比 QPS 数字重要得多。
  if (doVerify && mode != "get") {
    printf("\n[校验] 读回全部已写入的 key…\n");
    std::atomic<long long> missing{0};
    std::atomic<long long> wrong{0};
    std::vector<std::thread> vs;
    vs.reserve(static_cast<size_t>(nThreads));

    for (int t = 0; t < nThreads; ++t) {
      vs.emplace_back([&, t] {
        Clerk clerk;
        clerk.Init(cfg);                  // 校验用独立 Clerk，不复用压测线程的
        for (int i = 0; i < nOps; ++i) {
          if (mode == "mix" && i % 5 == 4) continue;   // mix 模式下这些位置没写入
          std::string got = clerk.Get(keyOf(t, i));    // Get 返回 "" 表示 ErrNoKey
          if (got.empty()) {
            ++missing;
          } else if (got != valOf(t, i)) {
            ++wrong;                       // 读到了别的值 —— 数据损坏
          }
        }
      });
    }
    for (auto& th : vs) th.join();

    long long checked = 0;
    for (int t = 0; t < nThreads; ++t)
      for (int i = 0; i < nOps; ++i)
        if (!(mode == "mix" && i % 5 == 4)) ++checked;

    printf("[校验] 检查 %lld 个 key：丢失 %lld，值错误 %lld\n", checked, missing.load(), wrong.load());
    if (missing.load() == 0 && wrong.load() == 0) {
      printf("[校验] ✅ 通过 —— 所有已确认的写入均可见且值正确\n");
    } else {
      printf("[校验] ❌ 失败 —— 存在丢写或数据损坏\n");
      printf("       提示：若\"值错误\"的返回值恰好等于 key 本身，\n");
      printf("       那是 skipList::load_file 的已知缺陷（值被写成了 key），见 src/skipList/skipList说明.md 疑点 1\n");
    }
  }

  return 0;
}
