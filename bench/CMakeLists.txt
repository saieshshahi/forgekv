add_executable(forgekv_benchmarks
  micro/protocol_benchmark.cpp
  micro/smoke_benchmark.cpp
)
target_link_libraries(forgekv_benchmarks PRIVATE
  forgekv_common
  forgekv_protocol
  benchmark::benchmark_main
)
