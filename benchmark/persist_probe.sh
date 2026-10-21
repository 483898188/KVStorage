#!/usr/bin/env bash
# 编译并运行 Persister 往返探针（在 /tmp 里跑，不污染仓库）
set -u
R=/home/hxl/KVstorageBaseRaft-cpp
W=/tmp/persist_probe

rm -rf "$W"
mkdir -p "$W"
cd "$W" || exit 1
cp "$R/benchmark/persist_probe.cpp" .

echo "=== 编译 ==="
g++ -std=c++20 \
  -I"$R/src/common/include" \
  -I"$R/src/raftCore/include" \
  -I"$R/src/raftRpcPro/include" \
  -I"$R/src/skipList/include" \
  -o probe persist_probe.cpp \
  "$R/src/raftCore/Persister.cpp" "$R/src/common/util.cpp" \
  -lpthread -lboost_serialization 2>&1 | head -25

if [ ! -x ./probe ]; then
  echo "!! 编译失败"
  exit 1
fi

echo
echo "=== 运行 ==="
./probe

echo
echo "=== 文件最终状态 ==="
ls -la raftstatePersist99.txt snapshotPersist99.txt 2>/dev/null
echo
echo "=== 文件原始内容（如果有）==="
head -c 200 raftstatePersist99.txt 2>/dev/null; echo
