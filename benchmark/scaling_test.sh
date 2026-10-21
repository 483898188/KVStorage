#!/usr/bin/env bash
# 实验：吞吐 vs 客户端并发数（集群只启动一次）
set -u
REPO=/home/hxl/KVstorageBaseRaft-cpp
BIN="$REPO/bin"
LOG=/tmp/raft_scaling_cluster.log
CPUSET="${CPUSET:-0-3}"
OPS="${OPS:-500}"

cd "$BIN" || exit 1
pkill -9 -f raftCoreRun 2>/dev/null; sleep 1
rm -f test.conf
: > "$LOG"

echo "=== 启动集群（绑核 $CPUSET）==="
taskset -c "$CPUSET" ./raftCoreRun -n 3 -f test.conf >"$LOG" 2>&1 &
PARENT=$!
trap 'kill -9 $PARENT 2>/dev/null; pkill -9 -f raftCoreRun 2>/dev/null' EXIT

for i in $(seq 1 25); do
  n=$(grep -c '^node' test.conf 2>/dev/null || true)
  [ "${n:-0}" -ge 6 ] && break
  sleep 1
done
sleep 12   # KvServer 内部还有 sleep(6)+sleep(nodeNum-me) 才 Raft::init

echo "=== 进程树（核实节点 pid 识别问题）==="
echo "父进程 CLUSTER_PID=$PARENT"
echo "pgrep -P \$PARENT  -> $(pgrep -P "$PARENT" 2>/dev/null | tr '\n' ' ')"
echo "pgrep -f raftCoreRun -> $(pgrep -f raftCoreRun 2>/dev/null | tr '\n' ' ')"
echo "ps -ef | grep raftCoreRun:"
ps -eo pid,ppid,comm,args 2>/dev/null | grep -E 'raftCoreRun' | grep -v grep | head -6

echo
echo "=== 吞吐 vs 并发 ==="
printf "%-8s %-10s %-12s %-10s %-10s\n" "线程" "操作数" "吞吐(ops/s)" "P50(ms)" "P99(ms)"
for T in 1 2 4 8; do
  out=$(taskset -c "$CPUSET" ./bench -t "$T" -n "$OPS" -w 20 -m put -f test.conf 2>&1)
  thr=$(echo "$out" | grep -oE '吞吐 [0-9]+' | grep -oE '[0-9]+')
  lat=$(echo "$out" | grep -E '^\[延迟\]')
  p50=$(echo "$lat" | grep -oE 'P50=[ ]*[0-9.]+' | grep -oE '[0-9.]+')
  p99=$(echo "$lat" | grep -oE 'P99=[ ]*[0-9.]+' | grep -oE '[0-9.]+')
  printf "%-8s %-10s %-12s %-10s %-10s\n" "$T" "$OPS" "${thr:-?}" "${p50:-?}" "${p99:-?}"
done

echo
echo "=== 最后一次运行的校验结果 ==="
echo "$out" | grep -E '^\[校验\]'
