add_library(forgekv_chaos_core
  artifacts.cpp
  client_worker.cpp
  fault_proxy.cpp
  harness.cpp
  process_cluster.cpp
  scheduler.cpp
  verifier.cpp
)
target_include_directories(forgekv_chaos_core PUBLIC ${PROJECT_SOURCE_DIR})
target_link_libraries(forgekv_chaos_core PUBLIC forgekv_net forgekv_protocol)

add_executable(forgekv_chaos main.cpp)
set_target_properties(forgekv_chaos PROPERTIES OUTPUT_NAME forgekv-chaos)
target_link_libraries(forgekv_chaos PRIVATE forgekv_chaos_core)

add_test(NAME ChaosCliHelp COMMAND forgekv_chaos --help)
add_test(NAME ChaosCliRejectsInvalidBounds COMMAND forgekv_chaos --nodes 4)
set_tests_properties(ChaosCliRejectsInvalidBounds PROPERTIES WILL_FAIL TRUE)
add_test(
  NAME ChaosCliSeededSmokeCleanup
  COMMAND ${CMAKE_COMMAND} -E remove_directory
          ${CMAKE_CURRENT_BINARY_DIR}/smoke-artifacts
)
set_tests_properties(ChaosCliSeededSmokeCleanup PROPERTIES
  FIXTURES_SETUP ChaosCliSeededSmokeFixture
)
add_test(
  NAME ChaosCliSeededSmoke
  COMMAND forgekv_chaos
    --nodes 3
    --clients 2
    --duration 1
    --action-interval-ms 250
    --seed 912831
    --server $<TARGET_FILE:forgekv_server>
    --artifacts ${CMAKE_CURRENT_BINARY_DIR}/smoke-artifacts
)
set_tests_properties(ChaosCliSeededSmoke PROPERTIES
  FIXTURES_REQUIRED ChaosCliSeededSmokeFixture
  TIMEOUT 60
)
add_dependencies(forgekv_chaos forgekv_server)
