#include "chaos/netem_runner.h"
#include "chaos/process_runner.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

namespace forgekv::chaos {
namespace {

using namespace std::chrono_literals;

class FakeExecutor final : public CommandExecutor {
 public:
  CommandResult run(const std::vector<std::string>& arguments,
                    const CommandOptions&) override {
    calls.push_back(arguments);
    if (results.empty()) {
      return {.exit_code = 0};
    }
    auto result = std::move(results.front());
    results.pop_front();
    return result;
  }

  std::vector<std::vector<std::string>> calls;
  std::deque<CommandResult> results;
};

NetemRunnerOptions options(const std::filesystem::path& output,
                           NetemProfile profile) {
  return NetemRunnerOptions{
      .chaos_path = "/opt/forgekv/forgekv-chaos",
      .server_path = "/opt/forgekv/forgekv-server",
      .output_directory = output,
      .profiles = {std::move(profile)},
      .nodes = 3U,
      .clients = 4U,
      .duration = 1s,
      .seed = 150015U,
  };
}

TEST(NetemRunnerTest, NamespaceAndInterfaceSafetyRulesAreNarrow) {
  EXPECT_TRUE(valid_netem_namespace_name("fkv-netem-123-0"));
  EXPECT_FALSE(valid_netem_namespace_name("default"));
  EXPECT_FALSE(valid_netem_namespace_name("fkv-netem-../host"));
  EXPECT_FALSE(valid_netem_namespace_name("fkv-netem-"));
  EXPECT_TRUE(netem_interface_is_safe("lo"));
  EXPECT_FALSE(netem_interface_is_safe("eth0"));
  EXPECT_FALSE(netem_interface_is_safe(""));
}

TEST(NetemRunnerTest, RefusesNonRootBeforeRunningAnyCommand) {
  FakeExecutor executor;
  NetemRunner runner(executor, 1000U, 42U);
  const auto result = runner.run(
      options("/tmp/forgekv-netem-test", NetemProfile{.name = "baseline"}));
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error.find("root"), std::string::npos);
  EXPECT_TRUE(executor.calls.empty());
}

TEST(NetemRunnerTest, BaselineCreatesRunsAndDeletesWithoutQdisc) {
  FakeExecutor executor;
  executor.results = {
      CommandResult{.exit_code = 0},
      CommandResult{.exit_code = 0},
      CommandResult{
          .exit_code = 0,
          .output = "result=pass converged=true restart_verified=true "
                    "attempts=50 acknowledged_writes=12 actions=0\n"},
      CommandResult{.exit_code = 0},
  };
  NetemRunner runner(executor, 0U, 42U);
  const auto result = runner.run(
      options("/tmp/forgekv-netem-test", NetemProfile{.name = "baseline"}));
  ASSERT_TRUE(result.ok()) << result.error;
  ASSERT_EQ(result.profiles.size(), 1U);
  EXPECT_TRUE(result.profiles.front().summary.passed);
  ASSERT_EQ(executor.calls.size(), 4U);
  EXPECT_EQ(executor.calls[0],
            (std::vector<std::string>{"ip", "netns", "add",
                                      "fkv-netem-42-0"}));
  EXPECT_EQ(executor.calls[1],
            (std::vector<std::string>{"ip", "netns", "exec",
                                      "fkv-netem-42-0", "ip", "link", "set",
                                      "lo", "up"}));
  EXPECT_EQ(executor.calls[2][0], "ip");
  EXPECT_EQ(executor.calls[2][4], "/opt/forgekv/forgekv-chaos");
  EXPECT_NE(std::ranges::find(executor.calls[2], "--no-chaos"),
            executor.calls[2].end());
  EXPECT_EQ(executor.calls[3],
            (std::vector<std::string>{"ip", "netns", "del",
                                      "fkv-netem-42-0"}));
}

TEST(NetemRunnerTest, AppliesKernelProfileCollectsStatsThenDeletes) {
  FakeExecutor executor;
  executor.results = {
      CommandResult{.exit_code = 0}, CommandResult{.exit_code = 0},
      CommandResult{.exit_code = 0},
      CommandResult{
          .exit_code = 0,
          .output = "result=pass converged=true restart_verified=true "
                    "attempts=40 acknowledged_writes=10 actions=0\n"},
      CommandResult{.exit_code = 0,
                    .output = " Sent 1000 bytes 10 pkt (dropped 2, overlimits 0 "
                              "requeues 0)\n"},
      CommandResult{.exit_code = 0},
  };
  NetemRunner runner(executor, 0U, 9U);
  const auto result = runner.run(options(
      "/tmp/forgekv-netem-test",
      NetemProfile{.name = "loss-1pct", .loss_basis_points = 100U}));
  ASSERT_TRUE(result.ok()) << result.error;
  ASSERT_EQ(executor.calls.size(), 6U);
  EXPECT_EQ(executor.calls[2],
            (std::vector<std::string>{"ip", "netns", "exec",
                                      "fkv-netem-9-0", "tc", "qdisc",
                                      "replace", "dev", "lo", "root", "netem",
                                      "loss", "1%"}));
  EXPECT_EQ(executor.calls[4],
            (std::vector<std::string>{"ip", "netns", "exec",
                                      "fkv-netem-9-0", "tc", "-s", "qdisc",
                                      "show", "dev", "lo"}));
  EXPECT_EQ(result.profiles.front().qdisc.packets, 10U);
  EXPECT_EQ(result.profiles.front().qdisc.dropped, 2U);
}

TEST(NetemRunnerTest, SetupFailureDeletesOnlyAnOwnedNamespace) {
  FakeExecutor executor;
  executor.results = {
      CommandResult{.exit_code = 0},
      CommandResult{.exit_code = 1, .output = "cannot enable loopback"},
      CommandResult{.exit_code = 0},
  };
  NetemRunner runner(executor, 0U, 7U);
  const auto result = runner.run(
      options("/tmp/forgekv-netem-test", NetemProfile{.name = "baseline"}));
  EXPECT_FALSE(result.ok());
  ASSERT_EQ(executor.calls.size(), 3U);
  EXPECT_EQ(executor.calls.back(),
            (std::vector<std::string>{"ip", "netns", "del",
                                      "fkv-netem-7-0"}));

  FakeExecutor add_failure;
  add_failure.results = {
      CommandResult{.exit_code = 1, .output = "already exists"},
  };
  NetemRunner second(add_failure, 0U, 7U);
  EXPECT_FALSE(second.run(options("/tmp/forgekv-netem-test",
                                  NetemProfile{.name = "baseline"}))
                   .ok());
  ASSERT_EQ(add_failure.calls.size(), 1U);
}

TEST(ProcessRunnerTest, CapturesOutputAndEnforcesTimeout) {
  PosixCommandExecutor executor;
  const auto echo = executor.run({"/bin/echo", "forgekv-netem"},
                                 CommandOptions{.timeout = 1s});
  ASSERT_TRUE(echo.ok()) << echo.error;
  EXPECT_EQ(echo.output, "forgekv-netem\n");

  const auto slow = executor.run({"/bin/sleep", "2"},
                                 CommandOptions{.timeout = 20ms});
  EXPECT_FALSE(slow.ok());
  EXPECT_TRUE(slow.timed_out);
}

}  // namespace
}  // namespace forgekv::chaos
