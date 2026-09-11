include(GoogleTest)

add_executable(forgekv_saturating_writer helpers/saturating_writer.cpp)

add_executable(forgekv_unit_tests
  unit/benchmark_workload_test.cpp
  unit/cluster_codecs_test.cpp
  unit/chaos_client_state_test.cpp
  unit/chaos_process_cluster_test.cpp
  unit/chaos_scheduler_test.cpp
  unit/chaos_verifier_test.cpp
  unit/chaos_artifacts_test.cpp
  unit/checksum_test.cpp
  unit/logging_test.cpp
  unit/kv_store_test.cpp
  unit/net_config_test.cpp
  unit/net_metrics_test.cpp
  unit/netem_test.cpp
  unit/netem_runner_test.cpp
  unit/observability_test.cpp
  unit/parser_test.cpp
  unit/persisted_raft_node_test.cpp
  unit/raft_node_test.cpp
  unit/raft_storage_test.cpp
  unit/raft_simulator_test.cpp
  unit/request_executor_test.cpp
  unit/serializer_test.cpp
  unit/snapshot_store_test.cpp
  unit/snapshot_codec_test.cpp
  unit/state_machine_test.cpp
  unit/smoke_test.cpp
  unit/wal_test.cpp
  unit/wal_record_test.cpp
)
target_link_libraries(forgekv_unit_tests PRIVATE
  forgekv_benchmark_core
  forgekv_common
  forgekv_cluster
  forgekv_chaos_core
  forgekv_net
  forgekv_protocol
  forgekv_raft
  forgekv_sim
  forgekv_storage
  GTest::gtest_main
)
target_include_directories(forgekv_unit_tests PRIVATE ${PROJECT_SOURCE_DIR})
target_compile_definitions(forgekv_unit_tests PRIVATE
  FORGEKV_SERVER_PATH="$<TARGET_FILE:forgekv_server>"
  FORGEKV_SATURATING_WRITER_PATH="$<TARGET_FILE:forgekv_saturating_writer>"
)
add_dependencies(forgekv_unit_tests forgekv_server forgekv_saturating_writer)

gtest_discover_tests(forgekv_unit_tests DISCOVERY_MODE PRE_TEST)

add_executable(forgekv_integration_tests
  integration/admin_server_test.cpp
  integration/chaos_harness_test.cpp
  integration/fault_proxy_test.cpp
  integration/netem_runner_test.cpp
  integration/real_cluster_test.cpp
  integration/storage_recovery_test.cpp
  integration/tcp_server_test.cpp
)
target_link_libraries(forgekv_integration_tests PRIVATE
  forgekv_chaos_core
  forgekv_cluster
  forgekv_net
  forgekv_storage
  GTest::gtest_main
)
target_compile_definitions(forgekv_integration_tests PRIVATE
  FORGEKV_SERVER_PATH="$<TARGET_FILE:forgekv_server>"
  FORGEKV_CHAOS_PATH="$<TARGET_FILE:forgekv_chaos>"
  FORGEKV_NETEM_PATH="$<TARGET_FILE:forgekv_netem>"
)
add_dependencies(forgekv_integration_tests forgekv_server forgekv_chaos forgekv_netem)

gtest_discover_tests(forgekv_integration_tests DISCOVERY_MODE PRE_TEST)

add_executable(forgekv_failure_tests
  failure/raft_persistence_crash_test.cpp
  failure/storage_crash_test.cpp
  failure/wal_recovery_test.cpp
)
target_link_libraries(forgekv_failure_tests PRIVATE
  forgekv_raft
  forgekv_storage
  GTest::gtest_main
)

gtest_discover_tests(forgekv_failure_tests DISCOVERY_MODE PRE_TEST)

add_executable(forgekv_storage_crash_helper
  failure/storage_crash_helper.cpp
)
target_link_libraries(forgekv_storage_crash_helper PRIVATE forgekv_storage)
target_compile_definitions(forgekv_failure_tests PRIVATE
  FORGEKV_CRASH_HELPER_PATH="$<TARGET_FILE:forgekv_storage_crash_helper>"
)
add_dependencies(forgekv_failure_tests forgekv_storage_crash_helper)

add_executable(forgekv_raft_crash_helper
  failure/raft_persistence_crash_helper.cpp
)
target_link_libraries(forgekv_raft_crash_helper PRIVATE forgekv_raft)
target_compile_definitions(forgekv_failure_tests PRIVATE
  FORGEKV_RAFT_CRASH_HELPER_PATH="$<TARGET_FILE:forgekv_raft_crash_helper>"
)
add_dependencies(forgekv_failure_tests forgekv_raft_crash_helper)

add_test(
  NAME RaftSimulatorCliSmoke
  COMMAND forgekv_raft_sim --seed 912831 --steps 1000
)
