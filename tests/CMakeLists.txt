include(GoogleTest)

add_executable(forgekv_unit_tests
  unit/checksum_test.cpp
  unit/logging_test.cpp
  unit/kv_store_test.cpp
  unit/net_config_test.cpp
  unit/net_metrics_test.cpp
  unit/parser_test.cpp
  unit/request_executor_test.cpp
  unit/serializer_test.cpp
  unit/smoke_test.cpp
  unit/wal_test.cpp
  unit/wal_record_test.cpp
)
target_link_libraries(forgekv_unit_tests PRIVATE
  forgekv_common
  forgekv_net
  forgekv_protocol
  forgekv_storage
  GTest::gtest_main
)

gtest_discover_tests(forgekv_unit_tests DISCOVERY_MODE PRE_TEST)

add_executable(forgekv_integration_tests
  integration/storage_recovery_test.cpp
  integration/tcp_server_test.cpp
)
target_link_libraries(forgekv_integration_tests PRIVATE
  forgekv_net
  forgekv_storage
  GTest::gtest_main
)

gtest_discover_tests(forgekv_integration_tests DISCOVERY_MODE PRE_TEST)

add_executable(forgekv_failure_tests
  failure/storage_crash_test.cpp
  failure/wal_recovery_test.cpp
)
target_link_libraries(forgekv_failure_tests PRIVATE
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
