cmake_minimum_required(VERSION 3.22)

project(ForgeKV VERSION 0.1.0 LANGUAGES CXX)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(ForgeKVOptions)
include(Dependencies)

add_subdirectory(src)

if(FORGEKV_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

if(FORGEKV_BUILD_BENCHMARKS)
  add_subdirectory(bench)
endif()
