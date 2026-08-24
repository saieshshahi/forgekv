include(GoogleTest)

add_executable(forgekv_unit_tests
  unit/checksum_test.cpp
  unit/logging_test.cpp
  unit/parser_test.cpp
  unit/serializer_test.cpp
  unit/smoke_test.cpp
)
target_link_libraries(forgekv_unit_tests PRIVATE
  forgekv_common
  forgekv_protocol
  GTest::gtest_main
)

gtest_discover_tests(forgekv_unit_tests DISCOVERY_MODE PRE_TEST)
