#!/usr/bin/env bash
# 决定性实验：同一个集群，先后用两个客户端进程写【同一个 key 空间】，
# 但用不同的值前缀区分。若第二个进程的写入被服务端当成重复请求丢弃，
# 校验会发现 key 上存的仍是第一个进程的值 -> "值错误"。
#
# 依据：Clerk::Uuid() 用 rand() 但从不 srand()，所以每个客户端进程的
#       clientId 序列是确定且相同的（src/raftClerk/include/clerk.h:26-28）。
set -u
REPO=/home/hxl/KVstorageBaseRaft-cpp
BIN="$REPO/bin"
CPUSET="${CPUSET:-0-3}"
OPS="${OPS:-300}"
LOG=/tmp/raft_dedup_cluster.log

cd "$BIN" || exit 1
pkill -9 -f raftCoreRun 2>/dev/null; sleep 1
rm -f test.conf; : > "$LOG"

echo "=== 启动集群（只启动这一次）==="
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
echo "############ 客户端进程 #1：值前缀 = A1 ############"
taskset -c "$CPUSET" ./bench -t 1 -n "$OPS" -w 20 -m put -p "A1_" -f test.conf 2>&1 \
  | grep -vE '^\[[0-9]{4}-' | grep -E '^\[结果\]|^\[延迟\]|^\[校验\]'

echo
echo "############ 客户端进程 #2：同一个集群，值前缀 = B2 ############"
taskset -c "$CPUSET" ./bench -t 1 -n "$OPS" -w 20 -m put -p "B2_" -f test.conf 2>&1 \
  | grep -vE '^\[[0-9]{4}-' | grep -E '^\[结果\]|^\[延迟\]|^\[校验\]'

echo
echo "############ 解读 ############"
echo "若进程 #2 的校验报【值错误 = $OPS】，说明它的写入全部被当成重复请求丢弃，"
echo "而客户端收到的是 OK —— 这是静默丢写（线性一致性被破坏）。"
echo "根因：Uuid() 未 srand()，两个客户端进程拿到相同 clientId。"
