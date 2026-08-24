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
