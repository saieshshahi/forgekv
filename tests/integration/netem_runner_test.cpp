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
#include <optional>
#include <string>
#include <sys/stat.h>
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

TEST(NetemIntegrationTest, CliRejectsUserWritableArtifactsParent) {
  if (::geteuid() != 0) {
    GTEST_SKIP() << "requires root to reach privileged CLI path validation";
  }
  using namespace std::chrono_literals;
  NetemDirectory directory;
  const auto unsafe_parent = directory.path() / "user-writable";
  std::error_code error;
  std::filesystem::create_directories(unsafe_parent, error);
  ASSERT_FALSE(error) << error.message();
  ASSERT_EQ(::chmod(unsafe_parent.c_str(), 0777), 0);

  PosixCommandExecutor executor;
  const auto result = executor.run(
      {FORGEKV_NETEM_PATH, "--chaos", FORGEKV_CHAOS_PATH, "--server",
       FORGEKV_SERVER_PATH, "--artifacts",
       (unsafe_parent / "new-output").string(), "--clients", "1",
       "--duration", "1", "--profile", "baseline"},
      CommandOptions{.timeout = 15s});
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.output.find("trusted root-owned parent"), std::string::npos)
      << result.output;
  EXPECT_FALSE(std::filesystem::exists(unsafe_parent / "new-output"));
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
  const auto actual_runner_hash = executor.run(
      {"sha256sum", "--", FORGEKV_NETEM_PATH}, CommandOptions{.timeout = 5s});
  ASSERT_TRUE(actual_runner_hash.ok()) << actual_runner_hash.error;
  ASSERT_GE(actual_runner_hash.output.size(), 64U);
  EXPECT_NE(environment.find("netem_sha256=" +
                             actual_runner_hash.output.substr(0U, 64U)),
            std::string::npos);

  std::ifstream results_input(output / "results.jsonl", std::ios::binary);
  ASSERT_TRUE(results_input.is_open());
  const std::string results((std::istreambuf_iterator<char>(results_input)),
                            std::istreambuf_iterator<char>());
  EXPECT_NE(results.find("\"profile\":\"baseline\""), std::string::npos);
  EXPECT_EQ(results.find("latency-"), std::string::npos);
}

TEST(NetemIntegrationTest, CliIgnoresAnUntrustedAmbientPath) {
  if (::geteuid() != 0) {
    GTEST_SKIP() << "requires root to reach privileged CLI tool validation";
  }
  using namespace std::chrono_literals;
  NetemDirectory directory;
  const auto output = directory.path() / "fixed-tools";
  PosixCommandExecutor executor;
  const auto result = executor.run(
      {"/usr/bin/env", "PATH=/definitely-missing", FORGEKV_NETEM_PATH,
       "--chaos", FORGEKV_CHAOS_PATH, "--server", FORGEKV_SERVER_PATH,
       "--artifacts", output.string(), "--profile", "baseline"},
      CommandOptions{.timeout = 30s});
  EXPECT_TRUE(result.ok()) << result.error << result.output;
  EXPECT_TRUE(std::filesystem::exists(output / "environment.txt"));
  EXPECT_TRUE(std::filesystem::exists(output / "results.jsonl"));
}

TEST(NetemIntegrationTest, CliFailsIfWorkloadExecutableChangesDuringRun) {
  if (::geteuid() != 0) {
    GTEST_SKIP() << "requires root and Linux network namespace capability";
  }
  using namespace std::chrono_literals;
  NetemDirectory directory;
  std::error_code error;
  std::filesystem::create_directories(directory.path(), error);
  ASSERT_FALSE(error) << error.message();
  const auto chaos_copy = directory.path() / "forgekv-chaos";
  std::filesystem::copy_file(FORGEKV_CHAOS_PATH, chaos_copy, error);
  ASSERT_FALSE(error) << error.message();
  std::filesystem::permissions(
      chaos_copy, std::filesystem::perms::owner_exec,
      std::filesystem::perm_options::add, error);
  ASSERT_FALSE(error) << error.message();

  const auto output = directory.path() / "identity-change";
  PosixCommandExecutor executor;
  std::optional<CommandResult> result;
  std::thread run([&] {
    result = executor.run(
        {FORGEKV_NETEM_PATH, "--chaos", chaos_copy.string(), "--server",
         FORGEKV_SERVER_PATH, "--artifacts", output.string(), "--clients", "1",
         "--duration", "1", "--profile", "baseline", "--profile",
         "latency-10ms"},
        CommandOptions{.timeout = 30s});
  });
  const auto children = output / "baseline" / "children.txt";
  const auto deadline = std::chrono::steady_clock::now() + 15s;
  while (!std::filesystem::exists(children) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(20ms);
  }
  if (!std::filesystem::exists(children)) {
    run.join();
    FAIL() << "workload children did not start";
    return;
  }
  const auto replacement = directory.path() / "replacement";
  std::filesystem::copy_file("/bin/true", replacement, error);
  if (error) {
    run.join();
    FAIL() << error.message();
    return;
  }
  std::filesystem::rename(replacement, chaos_copy, error);
  if (error) {
    run.join();
    FAIL() << error.message();
    return;
  }
  run.join();

  ASSERT_TRUE(result.has_value());
  EXPECT_FALSE(result->ok());
  EXPECT_NE(result->output.find("executable identity changed"),
            std::string::npos)
      << result->output;
  std::ifstream records(output / "results.jsonl");
  ASSERT_TRUE(records.is_open());
  const std::string evidence((std::istreambuf_iterator<char>(records)),
                             std::istreambuf_iterator<char>());
  std::size_t invalid_profiles = 0U;
  std::size_t offset = 0U;
  while ((offset = evidence.find("\"passed\":false", offset)) !=
         std::string::npos) {
    ++invalid_profiles;
    ++offset;
  }
  EXPECT_EQ(invalid_profiles, 2U) << evidence;
}

}  // namespace
}  // namespace forgekv::chaos
