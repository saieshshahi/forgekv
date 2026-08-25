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
