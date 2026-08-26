add_library(forgekv_common STATIC
  common/logging.cpp
)
target_link_libraries(forgekv_common PUBLIC forgekv_warnings forgekv_sanitizers)
target_include_directories(forgekv_common PUBLIC "${PROJECT_SOURCE_DIR}/src")

add_library(forgekv_protocol STATIC
  protocol/checksum.cpp
  protocol/parser.cpp
  protocol/serializer.cpp
)
target_link_libraries(forgekv_protocol PUBLIC forgekv_common)
target_include_directories(forgekv_protocol PUBLIC "${PROJECT_SOURCE_DIR}/src")

find_package(Threads REQUIRED)

add_library(forgekv_net STATIC
  net/metrics.cpp
  net/socket_ops.cpp
  server/request_executor.cpp
  server/tcp_server.cpp
)
target_link_libraries(forgekv_net PUBLIC forgekv_protocol Threads::Threads)
target_include_directories(forgekv_net PUBLIC "${PROJECT_SOURCE_DIR}/src")

add_library(forgekv_storage STATIC
  storage/kv_store.cpp
  storage/wal.cpp
  storage/wal_record.cpp
)
target_link_libraries(forgekv_storage PUBLIC forgekv_protocol Threads::Threads)
target_include_directories(forgekv_storage PUBLIC "${PROJECT_SOURCE_DIR}/src")

add_library(forgekv_raft STATIC
  raft/persisted_raft_node.cpp
  raft/raft_node.cpp
  raft/raft_storage.cpp
)
target_link_libraries(forgekv_raft PUBLIC forgekv_common forgekv_protocol)
target_include_directories(forgekv_raft PUBLIC "${PROJECT_SOURCE_DIR}/src")

add_library(forgekv_sim STATIC
  sim/raft_simulator.cpp
)
target_link_libraries(forgekv_sim PUBLIC forgekv_raft)
target_include_directories(forgekv_sim PUBLIC "${PROJECT_SOURCE_DIR}/src")

add_library(forgekv_cluster STATIC
  cluster/codecs.cpp
  cluster/node.cpp
)
target_link_libraries(forgekv_cluster PUBLIC forgekv_net forgekv_raft)
target_include_directories(forgekv_cluster PUBLIC "${PROJECT_SOURCE_DIR}/src")

add_executable(forgekv_server
  server/forgekv_server_main.cpp
)
set_target_properties(forgekv_server PROPERTIES OUTPUT_NAME "forgekv-server")
target_link_libraries(forgekv_server PRIVATE forgekv_cluster)

add_executable(forgekv_raft_sim
  sim/raft_sim_main.cpp
)
target_link_libraries(forgekv_raft_sim PRIVATE forgekv_sim)
