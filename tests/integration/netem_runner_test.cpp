#include "chaos/netem.h"
#include "chaos/netem_runner.h"
#include "chaos/process_runner.h"

#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <filesystem>
#include <string>
#include <sys/wait.h>
#include <thread>

#include <gtest/gtest.h>

namespace forgekv::chaos {
namespace {

class NetemDirectory final {
 public:
  NetemDirectory() {
    static std::atomic<std::uint64_t> sequence{0U};
    path_ = std::filesystem::temp_directory_path() /
            ("forgekv-netem-integration-" + std::to_string(::getpid()) + "-" +
             std::to_string(sequence.fetch_add(1U)));
  }
  ~NetemDirectory() { std::filesystem::remove_all(path_); }
  const std::filesystem::path& path() const noexcept { return path_; }

 private:
  std::filesystem::path path_;
};

TEST(NetemIntegrationTest,
     RealNamespaceHighLatencyAndLossConvergeAndRestart) {
  if (::geteuid() != 0) {
    GTEST_SKIP() << "requires root and Linux network namespace capability";
  }
  using namespace std::chrono_literals;
  NetemDirectory directory;
  PosixCommandExecutor executor;
  NetemRunner runner(executor, 0U, static_cast<std::uint64_t>(::getpid()));
  const auto result = runner.run(NetemRunnerOptions{
      .chaos_path = FORGEKV_CHAOS_PATH,
      .server_path = FORGEKV_SERVER_PATH,
      .output_directory = directory.path(),
      .profiles = {
          NetemProfile{.name = "latency-100ms", .delay_us = 100'000U},
          NetemProfile{.name = "loss-5pct", .loss_basis_points = 500U}},
      .nodes = 3U,
      .clients = 2U,
      .duration = 1s,
      .seed = 150015U,
  });
  ASSERT_TRUE(result.ok()) << result.error;
  ASSERT_EQ(result.profiles.size(), 2U);
  for (const auto& profile : result.profiles) {
    EXPECT_EQ(profile.summary.actions, 0U);
    EXPECT_GT(profile.summary.acknowledged_writes, 0U);
    EXPECT_TRUE(profile.summary.converged);
    EXPECT_TRUE(profile.summary.restart_verified);
  }
}

TEST(NetemIntegrationTest, SigtermStopsWorkloadAndDeletesOwnedNamespace) {
  if (::geteuid() != 0) {
    GTEST_SKIP() << "requires root and Linux network namespace capability";
  }
  using namespace std::chrono_literals;
  NetemDirectory directory;
  const auto child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    const auto output = directory.path().string();
    ::execl(FORGEKV_NETEM_PATH, "forgekv-netem", "--chaos",
            FORGEKV_CHAOS_PATH, "--server", FORGEKV_SERVER_PATH, "--artifacts",
            output.c_str(), "--nodes", "3", "--clients", "2", "--duration",
            "30", "--seed", "150015", "--profile", "latency-100ms", nullptr);
    ::_exit(126);
  }
  const auto namespace_path =
      std::filesystem::path("/run/netns") /
      ("fkv-netem-" + std::to_string(child) + "-0");
  const auto children_path =
      directory.path() / "latency-100ms" / "children.txt";
  const auto deadline = std::chrono::steady_clock::now() + 15s;
  while ((!std::filesystem::exists(namespace_path) ||
          !std::filesystem::exists(children_path)) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(20ms);
  }
  ASSERT_TRUE(std::filesystem::exists(namespace_path));
  ASSERT_TRUE(std::filesystem::exists(children_path));
  ASSERT_EQ(::kill(child, SIGTERM), 0);
  int status = 0;
  while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {
  }
  EXPECT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 1);
  EXPECT_FALSE(std::filesystem::exists(namespace_path));
}

}  // namespace
}  // namespace forgekv::chaos
