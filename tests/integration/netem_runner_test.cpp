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
#include <fstream>
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

TEST(NetemIntegrationTest, CliRejectsSymlinkedArtifactsAncestor) {
  if (::geteuid() != 0) {
    GTEST_SKIP() << "requires root to reach privileged CLI path validation";
  }
  using namespace std::chrono_literals;
  NetemDirectory directory;
  const auto target = directory.path() / "real-parent";
  const auto alias = directory.path() / "linked-parent";
  std::error_code error;
  std::filesystem::create_directories(target, error);
  ASSERT_FALSE(error) << error.message();
  std::filesystem::create_directory_symlink(target, alias, error);
  ASSERT_FALSE(error) << error.message();

  PosixCommandExecutor executor;
  const auto result = executor.run(
      {FORGEKV_NETEM_PATH, "--chaos", FORGEKV_CHAOS_PATH, "--server",
       FORGEKV_SERVER_PATH, "--artifacts", (alias / "new-output").string(),
       "--clients", "1", "--duration", "1", "--profile", "baseline"},
      CommandOptions{.timeout = 15s});
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.output.find("symlink"), std::string::npos) << result.output;
  EXPECT_FALSE(std::filesystem::exists(target / "new-output"));
}

TEST(NetemIntegrationTest, CliEvidenceIdentifiesEveryExecutableBuild) {
  if (::geteuid() != 0) {
    GTEST_SKIP() << "requires root and Linux network namespace capability";
  }
  using namespace std::chrono_literals;
  NetemDirectory directory;
  const auto output = directory.path() / "evidence";
  PosixCommandExecutor executor;
  const auto result = executor.run(
      {FORGEKV_NETEM_PATH, "--chaos", FORGEKV_CHAOS_PATH, "--server",
       FORGEKV_SERVER_PATH, "--artifacts", output.string(), "--clients", "1",
       "--duration", "1", "--profile", "baseline"},
      CommandOptions{.timeout = 30s});
  ASSERT_TRUE(result.ok()) << result.error << result.output;

  std::ifstream input(output / "environment.txt", std::ios::binary);
  ASSERT_TRUE(input.is_open());
  const std::string environment((std::istreambuf_iterator<char>(input)),
                                std::istreambuf_iterator<char>());
  EXPECT_NE(environment.find("netem_sha256="), std::string::npos);
  EXPECT_NE(environment.find("chaos_sha256="), std::string::npos);
  EXPECT_NE(environment.find("server_sha256="), std::string::npos);
  EXPECT_NE(environment.find("compiler="), std::string::npos);

  std::ifstream results_input(output / "results.jsonl", std::ios::binary);
  ASSERT_TRUE(results_input.is_open());
  const std::string results((std::istreambuf_iterator<char>(results_input)),
                            std::istreambuf_iterator<char>());
  EXPECT_NE(results.find("\"profile\":\"baseline\""), std::string::npos);
  EXPECT_EQ(results.find("latency-"), std::string::npos);
}

TEST(NetemIntegrationTest, CliFailsBeforeNamespaceWhenRequiredToolsAreMissing) {
  if (::geteuid() != 0) {
    GTEST_SKIP() << "requires root to reach privileged CLI tool validation";
  }
  using namespace std::chrono_literals;
  NetemDirectory directory;
  const auto output = directory.path() / "missing-tools";
  PosixCommandExecutor executor;
  const auto result = executor.run(
      {"/usr/bin/env", "PATH=/definitely-missing", FORGEKV_NETEM_PATH,
       "--chaos", FORGEKV_CHAOS_PATH, "--server", FORGEKV_SERVER_PATH,
       "--artifacts", output.string(), "--profile", "baseline"},
      CommandOptions{.timeout = 10s});
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.output.find("tools are unavailable"), std::string::npos)
      << result.output;
  EXPECT_FALSE(std::filesystem::exists(output / "environment.txt"));
  EXPECT_FALSE(std::filesystem::exists(output / "results.jsonl"));
}

}  // namespace
}  // namespace forgekv::chaos
