add_executable(forgekv_benchmarks
  micro/smoke_benchmark.cpp
)
target_link_libraries(forgekv_benchmarks PRIVATE
  forgekv_common
  benchmark::benchmark_main
)
