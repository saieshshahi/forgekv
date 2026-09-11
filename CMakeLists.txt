cmake_minimum_required(VERSION 3.22)

project(ForgeKV VERSION 0.1.0 LANGUAGES CXX)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(ForgeKVOptions)
include(Dependencies)

if(FORGEKV_BUILD_TESTS)
  enable_testing()
endif()

add_subdirectory(src)
add_subdirectory(chaos)

if(FORGEKV_BUILD_BENCHMARKS OR FORGEKV_BUILD_TESTS)
  add_subdirectory(bench)
endif()

if(FORGEKV_BUILD_TESTS)
  add_subdirectory(tests)
endif()

if(FORGEKV_BUILD_FUZZERS)
  add_subdirectory(fuzz)
endif()
