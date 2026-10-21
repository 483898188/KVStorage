#!/usr/bin/env bash
# 实验：固定 1 个客户端线程，改变【每轮操作数】，观察每操作成本是否随数据量增长。
# 目的：检验"DprintfKVDB + 每命令一次全量快照"是否真的带来 O(n²)。
# 集群只启动一次；各轮之间状态累积（日志增长），这正是要观察的对象。
set -u
REPO=/home/hxl/KVstorageBaseRaft-cpp
BIN="$REPO/bin"
LOG=/tmp/raft_opsscale_cluster.log
CPUSET="${CPUSET:-0-3}"

cd "$BIN" || exit 1
pkill -9 -f raftCoreRun 2>/dev/null; sleep 1
rm -f test.conf raftstatePersist*.txt snapshotPersist*.txt
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
sleep 12

echo
printf "%-10s %-12s %-10s %-10s %-10s\n" "本轮操作数" "吞吐(ops/s)" "P50(ms)" "P99(ms)" "max(ms)"
echo "-------------------------------------------------------------"

for N in 500 1500 3000; do
  out=$(taskset -c "$CPUSET" ./bench -t 1 -n "$N" -w 20 -m put -c -f test.conf 2>&1)
  thr=$(echo "$out" | grep -oE '吞吐 [0-9]+' | grep -oE '[0-9]+')
  lat=$(echo "$out" | grep -E '^\[延迟\]')
  p50=$(echo "$lat" | sed -E 's/.*P50=[ ]*([0-9.]+)ms.*/\1/')
  p99=$(echo "$lat" | sed -E 's/.*P99=[ ]*([0-9.]+)ms.*/\1/')
  mx=$(echo "$lat"  | sed -E 's/.*max=[ ]*([0-9.]+)ms.*/\1/')
  printf "%-10s %-12s %-10s %-10s %-10s\n" "$N" "${thr:-?}" "${p50:-?}" "${p99:-?}" "${mx:-?}"
done

echo
echo "=== 集群侧持久化文件大小（反映日志/快照规模）==="
ls -la raftstatePersist1.txt snapshotPersist1.txt 2>/dev/null
