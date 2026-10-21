#!/usr/bin/env bash
# 用系统 protoc(3.21.12) 重新生成 .pb.* ，使其与系统 libprotobuf 匹配。
# 注意：这会覆盖 git 跟踪的文件，可用 `git checkout -- <files>` 还原。
set -eu
REPO=/home/hxl/KVstorageBaseRaft-cpp
PROTOC=/usr/bin/protoc     # 3.21.12，与 /usr/lib/.../libprotobuf.so.32 匹配
WORK=/tmp/proto_regen

echo "使用: $($PROTOC --version)"

rm -rf "$WORK" && mkdir -p "$WORK"
cp "$REPO/src/rpc/rpcheader.proto"        "$WORK/"
cp "$REPO/src/raftRpcPro/raftRPC.proto"   "$WORK/"
cp "$REPO/src/raftRpcPro/kvServerRPC.proto" "$WORK/"

cd "$WORK"
for p in rpcheader raftRPC kvServerRPC; do
  echo "--- 生成 $p.proto ---"
  "$PROTOC" "$p.proto" --cpp_out=. 2>&1 | head -5
done
echo "生成结果:"
ls -la "$WORK"/*.pb.*

# ---- 放回仓库，注意 .pb.h 要进 include/，且 rpcheader 的 .cc 要改名成 .cpp ----
echo
echo "=== 安装到仓库 ==="

# rpc: rpcheader -> rpcheader.pb.cpp（仓库里的历史命名是 .cpp）+ include/rpcheader.pb.h
cp -f "$WORK/rpcheader.pb.cc" "$REPO/src/rpc/rpcheader.pb.cpp"
cp -f "$WORK/rpcheader.pb.h"  "$REPO/src/rpc/include/rpcheader.pb.h"
echo "OK src/rpc/rpcheader.pb.cpp, src/rpc/include/rpcheader.pb.h"

# raftRpcPro: raftRPC / kvServerRPC
cp -f "$WORK/raftRPC.pb.cc"    "$REPO/src/raftRpcPro/raftRPC.pb.cc"
cp -f "$WORK/raftRPC.pb.h"     "$REPO/src/raftRpcPro/include/raftRPC.pb.h"
cp -f "$WORK/kvServerRPC.pb.cc" "$REPO/src/raftRpcPro/kvServerRPC.pb.cc"
cp -f "$WORK/kvServerRPC.pb.h"  "$REPO/src/raftRpcPro/include/kvServerRPC.pb.h"
echo "OK raftRPC.pb.{cc,h}, kvServerRPC.pb.{cc,h}"

echo
echo "=== 验证：新生成物期望的运行时版本 ==="
grep -m2 -nE 'PROTOBUF_VERSION|MIN_PROTOC' "$REPO/src/rpc/include/rpcheader.pb.h"

echo
echo "=== 确认 mprpc 依赖的符号还在 ==="
grep -c 'CallMethod' "$REPO/src/raftRpcPro/include/raftRPC.pb.h" | sed 's/^/raftRPC.pb.h CallMethod: /'
grep -c '_Stub'      "$REPO/src/raftRpcPro/include/raftRPC.pb.h" | sed 's/^/raftRPC.pb.h Stub: /'
grep -c 'GetRequestPrototype' "$REPO/src/raftRpcPro/include/raftRPC.pb.h" | sed 's/^/GetRequestPrototype: /'
