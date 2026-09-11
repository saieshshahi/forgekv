add_library(forgekv_benchmark_core
  system/latency_histogram.cpp
  system/metrics_sampler.cpp
  system/workload.cpp
)
target_include_directories(forgekv_benchmark_core PUBLIC ${PROJECT_SOURCE_DIR})
target_link_libraries(forgekv_benchmark_core PUBLIC forgekv_common PRIVATE forgekv_chaos_core)

if(FORGEKV_BUILD_BENCHMARKS)
  add_executable(forgekv_benchmarks
    micro/protocol_benchmark.cpp
    micro/raft_benchmark.cpp
    micro/snapshot_benchmark.cpp
    micro/smoke_benchmark.cpp
    micro/storage_benchmark.cpp
  )
  target_link_libraries(forgekv_benchmarks PRIVATE
    forgekv_common
    forgekv_cluster
    forgekv_protocol
    forgekv_raft
    forgekv_storage
    benchmark::benchmark_main
  )

  add_executable(forgekv_load_generator
    client/load_generator.cpp
  )
  target_link_libraries(forgekv_load_generator PRIVATE forgekv_net)
endif()
