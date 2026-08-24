option(FORGEKV_BUILD_TESTS "Build ForgeKV tests" ON)
option(FORGEKV_BUILD_BENCHMARKS "Build ForgeKV benchmarks" ON)
option(FORGEKV_BUILD_FUZZERS "Build Clang libFuzzer targets" OFF)
option(FORGEKV_GLIBCXX_ASSERTIONS "Enable libstdc++ runtime assertions" OFF)

set(FORGEKV_SANITIZER "none" CACHE STRING "Sanitizer: none, address, undefined, or thread")
set_property(CACHE FORGEKV_SANITIZER PROPERTY STRINGS none address undefined thread)

if(NOT FORGEKV_SANITIZER MATCHES "^(none|address|undefined|thread)$")
  message(FATAL_ERROR "Unsupported FORGEKV_SANITIZER=${FORGEKV_SANITIZER}")
endif()

add_library(forgekv_warnings INTERFACE)
target_compile_features(forgekv_warnings INTERFACE cxx_std_20)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options(forgekv_warnings INTERFACE
    -Wall
    -Wextra
    -Wpedantic
    -Wconversion
    -Wshadow
  )
endif()

if(FORGEKV_GLIBCXX_ASSERTIONS)
  target_compile_definitions(forgekv_warnings INTERFACE _GLIBCXX_ASSERTIONS)
endif()

add_library(forgekv_sanitizers INTERFACE)
if(NOT FORGEKV_SANITIZER STREQUAL "none")
  if(NOT CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    message(FATAL_ERROR "ForgeKV sanitizers require GCC or Clang")
  endif()

  if(FORGEKV_SANITIZER STREQUAL "address")
    set(_forgekv_sanitize_flags -fsanitize=address -fno-omit-frame-pointer)
  elseif(FORGEKV_SANITIZER STREQUAL "undefined")
    set(_forgekv_sanitize_flags -fsanitize=undefined -fno-omit-frame-pointer)
  elseif(FORGEKV_SANITIZER STREQUAL "thread")
    set(_forgekv_sanitize_flags -fsanitize=thread -fno-omit-frame-pointer)
  endif()

  target_compile_options(forgekv_sanitizers INTERFACE ${_forgekv_sanitize_flags})
  target_link_options(forgekv_sanitizers INTERFACE ${_forgekv_sanitize_flags})
endif()

if(FORGEKV_BUILD_FUZZERS AND NOT CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  message(FATAL_ERROR "FORGEKV_BUILD_FUZZERS requires Clang")
endif()
