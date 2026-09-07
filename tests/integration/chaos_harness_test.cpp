#include "chaos/harness.h"

#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace forgekv::chaos {
namespace {

class HarnessDirectory final {
 public:
  HarnessDirectory() {
    static std::atomic<std::uint64_t> sequence{0U};
    path_ = std::filesystem::temp_directory_path() /
            ("forgekv-chaos-harness-" + std::to_string(::getpid()) + "-" +
             std::to_string(sequence.fetch_add(1U)));
  }
  ~HarnessDirectory() { std::filesystem::remove_all(path_); }
  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

 private:
  std::filesystem::path path_;
};

std::string read_text(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  return {std::istreambuf_iterator<char>(input),
          std::istreambuf_iterator<char>()};
}

pid_t start_chaos_cli(const std::filesystem::path& artifacts,
                      const std::string& duration,
                      const bool keep_success) {
  const auto child = ::fork();
  if (child != 0) return child;
  const auto artifact_text = artifacts.string();
  if (keep_success) {
    ::execl(FORGEKV_CHAOS_PATH, "forgekv-chaos", "--nodes", "3",
            "--clients", "2", "--duration", duration.c_str(),
            "--action-interval-ms", "250", "--seed", "777331",
            "--server", FORGEKV_SERVER_PATH, "--artifacts",
            artifact_text.c_str(), "--keep-success", nullptr);
  } else {
    ::execl(FORGEKV_CHAOS_PATH, "forgekv-chaos", "--nodes", "3",
            "--clients", "2", "--duration", duration.c_str(),
            "--action-interval-ms", "250", "--seed", "777331",
            "--server", FORGEKV_SERVER_PATH, "--artifacts",
            artifact_text.c_str(), nullptr);
  }
  ::_exit(126);
}

int wait_for_child(const pid_t child) {
  int status = 0;
  while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {
  }
  return status;
}

pid_t start_chaos_replay(const std::filesystem::path& timeline,
                         const std::filesystem::path& artifacts) {
  const auto child = ::fork();
  if (child != 0) return child;
  const auto timeline_text = timeline.string();
  const auto artifact_text = artifacts.string();
  ::execl(FORGEKV_CHAOS_PATH, "forgekv-chaos", "--replay",
          timeline_text.c_str(), "--server", FORGEKV_SERVER_PATH,
          "--artifacts", artifact_text.c_str(), "--keep-success", nullptr);
  ::_exit(126);
}

TEST(ChaosHarnessTest, ScriptedFailoverRestartsAndVerifiesAcknowledgedState) {
  using namespace std::chrono_literals;
  HarnessDirectory directory;
  HarnessOptions options{
      .node_count = 3U,
      .client_count = 4U,
      .duration = 2s,
      .action_interval = 500ms,
      .seed = 12345U,
      .server_path = FORGEKV_SERVER_PATH,
      .artifact_directory = directory.path(),
      .overall_timeout = 45s,
  };
  options.script = {
      ChaosAction{.kind = ActionKind::kill_follower,
                  .planned_offset_us = 250'000U},
      ChaosAction{.kind = ActionKind::restart_node,
                  .planned_offset_us = 700'000U},
      ChaosAction{.kind = ActionKind::partition_leader_majority,
                  .planned_offset_us = 1'050'000U},
      ChaosAction{.kind = ActionKind::heal_network,
                  .planned_offset_us = 1'500'000U},
  };

  const auto result = ChaosHarness(std::move(options)).run();
  ASSERT_TRUE(result.ok()) << result.diagnostic;
  EXPECT_GT(result.summary.acknowledged_writes, 0U);
  EXPECT_TRUE(result.summary.converged);
  EXPECT_TRUE(result.summary.restart_verified);
  EXPECT_TRUE(std::filesystem::exists(directory.path() / "config.json"));
  EXPECT_TRUE(std::filesystem::exists(directory.path() / "timeline.jsonl"));
  EXPECT_TRUE(std::filesystem::exists(directory.path() / "history.jsonl"));
  EXPECT_TRUE(std::filesystem::exists(directory.path() / "summary.json"));
  EXPECT_TRUE(std::filesystem::exists(directory.path() / "replay.txt"));
}

TEST(ChaosHarnessTest, StableWorkloadSchedulesNoFaultsAndStillVerifiesRestart) {
  using namespace std::chrono_literals;
  HarnessDirectory directory;
  HarnessOptions options{
      .node_count = 3U,
      .client_count = 3U,
      .duration = 1s,
      .action_interval = 250ms,
      .seed = 150015U,
      .server_path = FORGEKV_SERVER_PATH,
      .artifact_directory = directory.path(),
      .overall_timeout = 45s,
      .enable_chaos = false,
  };

  const auto result = ChaosHarness(std::move(options)).run();
  ASSERT_TRUE(result.ok()) << result.diagnostic;
  EXPECT_EQ(result.summary.actions, 0U);
  EXPECT_GT(result.summary.attempts, 0U);
  EXPECT_GT(result.summary.acknowledged_writes, 0U);
  EXPECT_TRUE(result.summary.converged);
  EXPECT_TRUE(result.summary.restart_verified);
  EXPECT_NE(read_text(directory.path() / "config.json")
                .find("\"chaos_enabled\":false"),
            std::string::npos);
}

TEST(ChaosHarnessTest, InterruptionUsesBoundedCleanupAndReapsChildren) {
  using namespace std::chrono_literals;
  HarnessDirectory directory;
  const auto start = std::chrono::steady_clock::now();
  HarnessOptions options{
      .node_count = 3U,
      .client_count = 2U,
      .duration = 10s,
      .action_interval = 500ms,
      .seed = 98765U,
      .server_path = FORGEKV_SERVER_PATH,
      .artifact_directory = directory.path(),
      .overall_timeout = 45s,
  };
  options.interrupted = [start] {
    return std::chrono::steady_clock::now() - start > 5s;
  };

  const auto result = ChaosHarness(std::move(options)).run();
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.summary.failure_category, "signal") << result.diagnostic;
  EXPECT_NE(result.summary.first_evidence.find("interrupted"), std::string::npos);
  for (std::size_t node = 1U; node <= 3U; ++node) {
    EXPECT_TRUE(std::filesystem::exists(
        directory.path() / "metrics" /
        ("node-" + std::to_string(node) + ".prom")));
  }
  std::ifstream children(directory.path() / "children.txt");
  std::vector<int> pids;
  for (int pid = -1; children >> pid;) pids.push_back(pid);
  ASSERT_EQ(pids.size(), 3U);
  for (const int pid : pids) {
    ASSERT_GT(pid, 0);
    errno = 0;
    EXPECT_EQ(::kill(pid, 0), -1);
    EXPECT_EQ(errno, ESRCH);
  }
}

TEST(ChaosHarnessTest, ScriptedPauseLatencyLossAndHealConverges) {
  using namespace std::chrono_literals;
  HarnessDirectory directory;
  HarnessOptions options{
      .node_count = 3U,
      .client_count = 3U,
      .duration = 2s,
      .action_interval = 250ms,
      .seed = 424242U,
      .server_path = FORGEKV_SERVER_PATH,
      .artifact_directory = directory.path(),
      .overall_timeout = 45s,
  };
  options.script = {
      ChaosAction{.kind = ActionKind::set_latency,
                  .planned_offset_us = 200'000U,
                  .node = 1U,
                  .peer = 2U,
                  .value = 20U},
      ChaosAction{.kind = ActionKind::set_loss,
                  .planned_offset_us = 500'000U,
                  .node = 2U,
                  .peer = 3U,
                  .value = 10U},
      ChaosAction{.kind = ActionKind::pause_node,
                  .planned_offset_us = 800'000U,
                  .node = 1U},
      ChaosAction{.kind = ActionKind::resume_node,
                  .planned_offset_us = 1'150'000U,
                  .node = 1U},
      ChaosAction{.kind = ActionKind::heal_network,
                  .planned_offset_us = 1'500'000U},
  };

  const auto result = ChaosHarness(std::move(options)).run();
  ASSERT_TRUE(result.ok()) << result.diagnostic;
  EXPECT_EQ(result.summary.actions, 5U);
  EXPECT_TRUE(result.summary.restart_verified);
}

TEST(ChaosHarnessTest, RapidLeaderChurnKillsTwoSuccessiveLeadersAndRecovers) {
  using namespace std::chrono_literals;
  HarnessDirectory directory;
  HarnessOptions options{
      .node_count = 3U,
      .client_count = 2U,
      .duration = 3s,
      .action_interval = 500ms,
      .seed = 8675309U,
      .server_path = FORGEKV_SERVER_PATH,
      .artifact_directory = directory.path(),
      .overall_timeout = 50s,
  };
  options.script = {
      ChaosAction{.kind = ActionKind::rapid_leader_churn,
                  .planned_offset_us = 250'000U},
  };

  const auto result = ChaosHarness(std::move(options)).run();
  ASSERT_TRUE(result.ok()) << result.diagnostic;
  EXPECT_EQ(result.summary.actions, 1U);
  EXPECT_TRUE(result.summary.converged);
  EXPECT_TRUE(result.summary.restart_verified);
}

TEST(ChaosHarnessTest, CliSigtermReapsServersAndRetainsFailureEvidence) {
  using namespace std::chrono_literals;
  HarnessDirectory directory;
  const auto child = start_chaos_cli(directory.path(), "30", true);
  ASSERT_GT(child, 0);
  const auto deadline = std::chrono::steady_clock::now() + 15s;
  while (!std::filesystem::exists(directory.path() / "children.txt") &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(25ms);
  }
  const bool started =
      std::filesystem::exists(directory.path() / "children.txt");
  EXPECT_TRUE(started);
  EXPECT_EQ(::kill(child, SIGTERM), 0);
  const auto status = wait_for_child(child);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 1);
  EXPECT_NE(read_text(directory.path() / "summary.json")
                .find("\"failure_category\":\"signal\""),
            std::string::npos);
  std::ifstream children(directory.path() / "children.txt");
  for (int pid = -1; children >> pid;) {
    errno = 0;
    EXPECT_EQ(::kill(pid, 0), -1);
    EXPECT_EQ(errno, ESRCH);
  }
  for (std::size_t node = 1U; node <= 3U; ++node) {
    EXPECT_TRUE(std::filesystem::exists(
        directory.path() / "metrics" /
        ("node-" + std::to_string(node) + ".prom")));
  }
}

TEST(ChaosHarnessTest, CliSuccessRetainsOnlyCompactEvidenceByDefault) {
  HarnessDirectory directory;
  const auto child = start_chaos_cli(directory.path(), "1", false);
  ASSERT_GT(child, 0);
  const auto status = wait_for_child(child);
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_EQ(WEXITSTATUS(status), 0);
  EXPECT_TRUE(std::filesystem::exists(directory.path() / "config.json"));
  EXPECT_TRUE(std::filesystem::exists(directory.path() / "seed.txt"));
  EXPECT_TRUE(std::filesystem::exists(directory.path() / "summary.json"));
  EXPECT_FALSE(std::filesystem::exists(directory.path() / "data"));
  EXPECT_FALSE(std::filesystem::exists(directory.path() / "logs"));
  EXPECT_FALSE(std::filesystem::exists(directory.path() / "metrics"));
  EXPECT_FALSE(std::filesystem::exists(directory.path() / "history.jsonl"));
  EXPECT_FALSE(std::filesystem::exists(directory.path() / "timeline.jsonl"));
  EXPECT_FALSE(std::filesystem::exists(directory.path() / "replay.txt"));
}

TEST(ChaosHarnessTest, CliReplayRestoresOriginalNondefaultSeedAndShape) {
  HarnessDirectory root;
  const auto source = root.path() / "source";
  const auto replay = root.path() / "replay";
  const auto source_child = start_chaos_cli(source, "1", true);
  ASSERT_GT(source_child, 0);
  auto status = wait_for_child(source_child);
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_EQ(WEXITSTATUS(status), 0);

  const auto replay_child =
      start_chaos_replay(source / "timeline.jsonl", replay);
  ASSERT_GT(replay_child, 0);
  status = wait_for_child(replay_child);
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_EQ(WEXITSTATUS(status), 0);
  const auto config = read_text(replay / "config.json");
  EXPECT_NE(config.find("\"nodes\":3"), std::string::npos);
  EXPECT_NE(config.find("\"clients\":2"), std::string::npos);
  EXPECT_NE(config.find("\"duration_ms\":1000"), std::string::npos);
  EXPECT_NE(config.find("\"action_interval_ms\":250"), std::string::npos);
  EXPECT_NE(config.find("\"seed\":777331"), std::string::npos);
}

}  // namespace
}  // namespace forgekv::chaos
