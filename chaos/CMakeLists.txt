add_library(forgekv_chaos_core
  artifacts.cpp
  client_worker.cpp
  fault_proxy.cpp
  harness.cpp
  netem.cpp
  netem_runner.cpp
  process_runner.cpp
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

add_executable(forgekv_netem netem_main.cpp)
set_target_properties(forgekv_netem PROPERTIES OUTPUT_NAME forgekv-netem)
target_link_libraries(forgekv_netem PRIVATE forgekv_chaos_core)
add_dependencies(forgekv_netem forgekv_chaos forgekv_server)

add_test(NAME NetemCliHelp COMMAND forgekv_netem --help)
add_test(NAME NetemCliRejectsInvalidBounds COMMAND forgekv_netem --nodes 4)
set_tests_properties(NetemCliRejectsInvalidBounds PROPERTIES WILL_FAIL TRUE)
add_test(NAME NetemCliRejectsDuplicateOptions
  COMMAND forgekv_netem --nodes 3 --nodes 5)
set_tests_properties(NetemCliRejectsDuplicateOptions PROPERTIES WILL_FAIL TRUE)
add_test(NAME NetemCliRejectsUnknownProfile
  COMMAND forgekv_netem --profile not-a-profile)
set_tests_properties(NetemCliRejectsUnknownProfile PROPERTIES WILL_FAIL TRUE)
add_test(NAME NetemCliRejectsMissingRequiredPaths COMMAND forgekv_netem)
set_tests_properties(NetemCliRejectsMissingRequiredPaths PROPERTIES WILL_FAIL TRUE)
