# benchmark 模块：压测客户端
# 依赖关系与 example/raftCoreExample 的 callerMain 一致：
#   - src_raftClerk 提供 Clerk / raftServerRpcUtil（注意 skip_list_on_raft 库里【不含】它）
#   - src_common   提供 Op / util.h
#   - skip_list_on_raft 提供其余全部（含 mprpc、protobuf 符号）
#
# 注意：新增/删除本目录下的源文件后必须重新执行 cmake 配置，
#       因为 aux_source_directory 只在配置阶段扫描目录（见 CLAUDE.md）。

set(SRC_LIST bench.cpp)

add_executable(bench ${SRC_LIST} ${src_raftClerk} ${src_common})
target_link_libraries(bench skip_list_on_raft protobuf boost_serialization pthread)
