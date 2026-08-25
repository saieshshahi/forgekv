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
  raft/raft_node.cpp
)
target_link_libraries(forgekv_raft PUBLIC forgekv_common)
target_include_directories(forgekv_raft PUBLIC "${PROJECT_SOURCE_DIR}/src")
