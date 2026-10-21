#!/usr/bin/env bash
#
# benchmark/run_bench.sh —— 一键压测：起集群 → 跑压测 → 收集群
#
# 面向 4 核 8G 预算设计：
#   - 默认用 taskset 把整个集群 + 客户端绑到 4 个核上（CPUSET=0-3）
#   - 3 节点本身在 4 核上已经接近满载，所以客户端线程数默认给得很保守
#
# 用法:
#   ./benchmark/run_bench.sh                    # 默认参数
#   THREADS=4 OPS=5000 ./benchmark/run_bench.sh # 覆盖参数
#   FAULT=kill ./benchmark/run_bench.sh         # 压测中杀掉一个节点
#
set -u

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="$REPO_ROOT/bin"
CFG_NAME="test.conf"
CLUSTER_LOG="/tmp/raft_bench_cluster.log"

# ---------------- 可调参数 ----------------
NODES="${NODES:-3}"
THREADS="${THREADS:-2}"
OPS="${OPS:-2000}"
MODE="${MODE:-put}"
WARMUP="${WARMUP:-100}"
CPUSET="${CPUSET:-0-3}"          # 只允许用 4 个核；设为 "" 则不绑核
STARTUP_WAIT="${STARTUP_WAIT:-20}"   # 集群启动等待秒数（KvServer 内部有 sleep(6) + 依赖节点数）
FAULT="${FAULT:-none}"           # none | stop | cont | kill
FAULT_DELAY="${FAULT_DELAY:-10}" # 压测开始后多少秒注入故障
FAULT_NODE="${FAULT_NODE:-1}"    # 对哪个节点下手（0-based，默认 1；3 节点时通常不是 leader）

# ---------------- 工具函数 ----------------
say()  { printf '\033[1;36m[bench]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[警告]\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m[错误]\033[0m %s\n' "$*" >&2; exit 1; }

# 绑核前缀。用数组而不是 shell 函数：
# 把函数放后台（`run_pinned ... &`）会 fork 一个子 shell，$! 拿到的是【子 shell】的 pid，
# 于是 pgrep -P 只能看到一个中间进程，故障注入会打错目标。直接后台化命令才拿得到真 pid。
PIN=()
if [[ -n "$CPUSET" ]] && command -v taskset >/dev/null 2>&1; then
  PIN=(taskset -c "$CPUSET")
fi

CLEANUP_PIDS=()
cleanup() {
  echo
  say "清理集群…"
  for pid in "${CLEANUP_PIDS[@]:-}"; do
    [[ -n "${pid:-}" ]] && kill -9 "$pid" 2>/dev/null
  done
  pkill -9 -f 'raftCoreRun' 2>/dev/null
  sleep 1
  say "已清理。集群日志: $CLUSTER_LOG"
}
trap cleanup EXIT INT TERM

# ---------------- 0. 前置检查 ----------------
say "前置检查…"

[[ -x "$BIN_DIR/raftCoreRun" ]] || die "找不到 $BIN_DIR/raftCoreRun，请先构建：
  mkdir -p cmake-build-debug && cmake -S . -B cmake-build-debug && cmake --build cmake-build-debug -j"
[[ -x "$BIN_DIR/bench" ]] || die "找不到 $BIN_DIR/bench，请先构建（同上；若已构建过，注意新增源文件需重新执行 cmake 配置）"

# --- 关键：检查两个必须调整的配置，否则测的不是 Raft ---
CFG_H="$REPO_ROOT/src/common/include/config.h"
if grep -qE '^\s*const bool Debug\s*=\s*true' "$CFG_H"; then
  warn "src/common/include/config.h 里 Debug 仍为 true。"
  warn "  影响：每 apply 一条命令就调用 display_list() 打印整个跳表（O(n)），总复杂度 O(n²)。"
  warn "  这会完全淹没 Raft 本身的开销。建议改为 false 后重新构建。"
  echo
fi

DBSRC="$REPO_ROOT/example/raftCoreExample/raftKvDB.cpp"
MAXSTATE="$(grep -oE 'KvServer\([a-zA-Z0-9_]+,\s*[0-9-]+' "$DBSRC" | grep -oE '[0-9-]+$' || echo '?')"
say "当前 maxraftstate = $MAXSTATE"
if [[ "$MAXSTATE" == "500" ]]; then
  warn "  maxraftstate=500 意味着快照阈值 = 500/10 = 50 字节。"
  warn "  而 persistData() 光是 term/votedFor/快照元信息就超过 50 字节，"
  warn "  结果是【几乎每 apply 一条命令就触发一次全量快照】（转储整个跳表 + 写文件）。"
  warn "  建议改成 1MB 左右（见 README 的说明），或 -1 完全关闭快照以隔离变量。"
  echo
fi

# ---------------- 1. 起集群 ----------------
say "启动 $NODES 节点集群（绑核: ${CPUSET:-无}）…"
cd "$BIN_DIR" || die "无法进入 $BIN_DIR"

# 必须清空：否则历史端口会残留在配置里（见 src/rpc/rpc说明.md 疑点 4）
rm -f "$CFG_NAME"

# 持久化状态。修复 Persister 之后落盘状态会真的在重启后被读回，
# 于是反复跑压测会继承上一轮的数据和日志（raftstate 持续增长，影响可比性）。
# 默认清掉以保证每轮都是干净起点；设 CLEAN_STATE=0 可保留，用于验证恢复路径。
if [[ "${CLEAN_STATE:-1}" == "1" ]]; then
  rm -f raftstatePersist*.txt snapshotPersist*.txt
  say "已清空持久化状态（CLEAN_STATE=0 可保留）"
fi

: > "$CLUSTER_LOG"

"${PIN[@]}" ./raftCoreRun -n "$NODES" -f "$CFG_NAME" >"$CLUSTER_LOG" 2>&1 &
CLUSTER_PID=$!
CLEANUP_PIDS+=("$CLUSTER_PID")

# 轮询等待配置写全（每个节点写 2 行）
say "等待集群就绪（最多 ${STARTUP_WAIT}s）…"
# 注意：grep -c 在"文件存在但 0 匹配"时会输出 "0" 且返回非零，
#       所以不能用 `|| echo 0`（会拼成 "0\n0" 让 [[ -ge ]] 算术报错），要用 `|| true`
count_nodes() {
  local n
  n=$(grep -c '^node' "$CFG_NAME" 2>/dev/null || true)
  echo "${n:-0}"
}
for ((i = 0; i < STARTUP_WAIT; i++)); do
  if [[ "$(count_nodes)" -ge $((NODES * 2)) ]]; then break; fi
  sleep 1
done
lines=$(count_nodes)
if [[ "$lines" -lt $((NODES * 2)) ]]; then
  warn "配置文件只写了 $lines 行（期望 $((NODES * 2))）。集群日志："
  tail -20 "$CLUSTER_LOG"
  die "集群未能正常启动"
fi
# 配置文件写完 ≠ 可以服务：KvServer 构造里还有 sleep(6) 和 sleep(nodeCount - me)，
# 之后才 Raft::init 并选举。这里留足时间，避免把冷启动算进压测。
sleep 10

# 取出各节点 pid，供故障注入使用。
#
# 注意：不能从启动日志里 grep "pid:" —— raftKvDB.cpp 在 fork() 之前调用 getpid()，
# 所以那行打印的永远是【父进程】的 pid（三次完全相同）。必须靠进程树来定子进程。
mapfile -t NODE_PIDS < <(pgrep -P "$CLUSTER_PID" 2>/dev/null | sort -n)
if [[ ${#NODE_PIDS[@]} -lt 1 ]]; then
  # 退路：按名字找，排除父进程
  mapfile -t NODE_PIDS < <(pgrep -f 'raftCoreRun' 2>/dev/null | grep -v "^${CLUSTER_PID}$" | sort -n)
fi
say "集群父进程 pid: $CLUSTER_PID"
say "节点 pid: ${NODE_PIDS[*]:-未识别}"
if [[ ${#NODE_PIDS[@]} -lt "$NODES" ]]; then
  warn "只识别出 ${#NODE_PIDS[@]} 个节点进程（期望 $NODES），故障注入可能不准"
fi

# ---------------- 2. 故障注入（可选，后台）----------------
FAULT_PID=""
if [[ "$FAULT" != "none" ]]; then
  target="${NODE_PIDS[$FAULT_NODE]:-}"
  if [[ -z "$target" ]]; then
    warn "FAULT=$FAULT 但拿不到节点 $FAULT_NODE 的 pid，跳过注入"
  else
    say "已安排：压测开始 ${FAULT_DELAY}s 后对 node$FAULT_NODE (pid $target) 执行 $FAULT"
    (
      sleep "$((FAULT_DELAY + 3))"   # +3 让预热先跑完
      case "$FAULT" in
        kill) say ">>> 注入故障：kill -9 node$FAULT_NODE (pid $target)"; kill -9 "$target" 2>/dev/null ;;
        stop) say ">>> 注入故障：kill -STOP node$FAULT_NODE (pid $target)"; kill -STOP "$target" 2>/dev/null ;;
        cont) say ">>> 注入故障：kill -CONT node$FAULT_NODE (pid $target)"; kill -CONT "$target" 2>/dev/null ;;
      esac
    ) &
    FAULT_PID=$!
  fi
fi

# ---------------- 3. 跑压测 ----------------
echo
say "开始压测：threads=$THREADS ops/thread=$OPS mode=$MODE"
echo "──────────────────────────────────────────────────────────"
"${PIN[@]}" ./bench -t "$THREADS" -n "$OPS" -w "$WARMUP" -m "$MODE" -f "$CFG_NAME"
BENCH_RC=$?
echo "──────────────────────────────────────────────────────────"

[[ -n "$FAULT_PID" ]] && kill -9 "$FAULT_PID" 2>/dev/null

# ---------------- 4. 结论 ----------------
echo
say "压测结束 (exit=$BENCH_RC)"
case "$FAULT" in
  none) say "未注入故障 —— 这是理想条件下的基线" ;;
  *)    say "已注入故障 FAULT=$FAULT，请对照基线看 QPS 与 P99 的变化" ;;
esac

exit $BENCH_RC
