#include "chaos/netem.h"
#include "chaos/netem_runner.h"
#include "chaos/process_runner.h"

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>

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

TEST(NetemIntegrationTest, RealNamespaceBaselineConvergesAndRestarts) {
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
      .profiles = {NetemProfile{.name = "baseline"}},
      .nodes = 3U,
      .clients = 2U,
      .duration = 1s,
      .seed = 150015U,
  });
  ASSERT_TRUE(result.ok()) << result.error;
  ASSERT_EQ(result.profiles.size(), 1U);
  EXPECT_EQ(result.profiles.front().summary.actions, 0U);
  EXPECT_GT(result.profiles.front().summary.acknowledged_writes, 0U);
  EXPECT_TRUE(result.profiles.front().summary.converged);
  EXPECT_TRUE(result.profiles.front().summary.restart_verified);
}

}  // namespace
}  // namespace forgekv::chaos
