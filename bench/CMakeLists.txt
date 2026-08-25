add_executable(forgekv_benchmarks
  micro/protocol_benchmark.cpp
  micro/smoke_benchmark.cpp
)
target_link_libraries(forgekv_benchmarks PRIVATE
  forgekv_common
  forgekv_protocol
  benchmark::benchmark_main
)

add_executable(forgekv_load_generator
  client/load_generator.cpp
)
target_link_libraries(forgekv_load_generator PRIVATE forgekv_net)
