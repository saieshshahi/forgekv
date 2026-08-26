include(GoogleTest)

add_executable(forgekv_unit_tests
  unit/cluster_codecs_test.cpp
  unit/checksum_test.cpp
  unit/logging_test.cpp
  unit/kv_store_test.cpp
  unit/net_config_test.cpp
  unit/net_metrics_test.cpp
  unit/parser_test.cpp
  unit/persisted_raft_node_test.cpp
  unit/raft_node_test.cpp
  unit/raft_storage_test.cpp
  unit/raft_simulator_test.cpp
  unit/request_executor_test.cpp
  unit/serializer_test.cpp
  unit/smoke_test.cpp
  unit/wal_test.cpp
  unit/wal_record_test.cpp
)
target_link_libraries(forgekv_unit_tests PRIVATE
  forgekv_common
  forgekv_cluster
  forgekv_net
  forgekv_protocol
  forgekv_raft
  forgekv_sim
  forgekv_storage
  GTest::gtest_main
)

gtest_discover_tests(forgekv_unit_tests DISCOVERY_MODE PRE_TEST)

add_executable(forgekv_integration_tests
  integration/real_cluster_test.cpp
  integration/storage_recovery_test.cpp
  integration/tcp_server_test.cpp
)
target_link_libraries(forgekv_integration_tests PRIVATE
  forgekv_cluster
  forgekv_net
  forgekv_storage
  GTest::gtest_main
)
target_compile_definitions(forgekv_integration_tests PRIVATE
  FORGEKV_SERVER_PATH="$<TARGET_FILE:forgekv_server>"
)
add_dependencies(forgekv_integration_tests forgekv_server)

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
