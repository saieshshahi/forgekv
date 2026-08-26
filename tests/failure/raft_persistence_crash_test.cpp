#include "raft/persisted_raft_node.h"

#include <gtest/gtest.h>

#include <atomic>
#include <csignal>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include <sys/wait.h>
#include <unistd.h>

namespace forgekv::raft {
namespace {

class CrashDirectory final {
 public:
  CrashDirectory() {
    static std::atomic<std::uint64_t> sequence{0};
    path_ = std::filesystem::temp_directory_path() /
            ("forgekv-raft-crash-" + std::to_string(::getpid()) + "-" +
             std::to_string(sequence.fetch_add(1)));
    std::filesystem::create_directories(path_);
  }
  ~CrashDirectory() { std::filesystem::remove_all(path_); }
  [[nodiscard]] const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

RaftConfig config() {
  return RaftConfig{
      .self_id = 1,
      .voters = {1, 2, 3},
      .election_timeout_min = 100,
      .election_timeout_max = 200,
      .heartbeat_interval = 25,
      .random_seed = 811,
  };
}

RaftStorage open_storage(const std::filesystem::path& directory) {
  return RaftStorage::open(directory, 1, 1, {},
                           fixed_membership_fingerprint(config().voters));
}

void run_helper(const std::filesystem::path& directory, const char* mode,
                const char* point, const char* occurrence = "1") {
  const auto child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    ::execl(FORGEKV_RAFT_CRASH_HELPER_PATH,
            FORGEKV_RAFT_CRASH_HELPER_PATH, directory.c_str(), mode, point,
            occurrence, nullptr);
    ::_exit(126);
  }
  int status = 0;
  ASSERT_EQ(::waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFSIGNALED(status)) << point;
  EXPECT_EQ(WTERMSIG(status), SIGKILL) << point;
}

TEST(RaftPersistenceCrashTest, VoteTransitionIsSafeAtEveryBoundary) {
  struct Case final {
    const char* point;
    const char* occurrence;
    bool must_have_term;
    bool must_have_vote;
    bool response_visible;
  };
  for (const auto& test_case : {
           Case{"before_persist", "1", false, false, false},
           Case{"after_write", "1", false, false, false},
           Case{"after_file_sync", "1", false, false, false},
           Case{"after_rename", "1", true, false, false},
           Case{"after_sync", "1", true, false, false},
           Case{"before_persist", "2", true, false, false},
           Case{"after_write", "2", true, false, false},
           Case{"after_file_sync", "2", true, false, false},
           Case{"after_rename", "2", true, true, false},
           Case{"after_sync", "2", true, true, false},
           Case{"before_response", "1", true, true, false},
           Case{"after_response", "1", true, true, true},
       }) {
    CrashDirectory directory;
    run_helper(directory.path(), "vote", test_case.point,
               test_case.occurrence);
    const bool marker =
        std::filesystem::exists(directory.path() / "response.marker");
    EXPECT_EQ(marker, test_case.response_visible) << test_case.point;

    auto storage = open_storage(directory.path());
    if (test_case.must_have_term) {
      EXPECT_EQ(storage.state().current_term, 1U) << test_case.point;
    }
    if (test_case.must_have_vote) {
      EXPECT_EQ(storage.state().voted_for, 2U) << test_case.point;
    }
    if (marker) {
      EXPECT_EQ(storage.state().current_term, 1U);
      EXPECT_EQ(storage.state().voted_for, 2U);
    }
  }
}

TEST(RaftPersistenceCrashTest, AppendTransitionIsSafeAtEveryBoundary) {
  struct Case final {
    const char* point;
    bool must_have_entry;
    bool response_visible;
  };
  for (const auto& test_case : {
           Case{"before_persist", false, false},
           Case{"after_write", false, false},
           Case{"after_sync", true, false},
           Case{"before_response", true, false},
           Case{"after_response", true, true},
       }) {
    CrashDirectory directory;
    run_helper(directory.path(), "log", test_case.point);
    const bool marker =
        std::filesystem::exists(directory.path() / "response.marker");
    EXPECT_EQ(marker, test_case.response_visible) << test_case.point;

    auto storage = open_storage(directory.path());
    if (test_case.must_have_entry || marker) {
      ASSERT_EQ(storage.state().log.size(), 1U) << test_case.point;
      EXPECT_EQ(storage.state().log.front().term, 2U);
      EXPECT_EQ(storage.state().log.front().command,
                (std::vector<std::byte>{std::byte{0x5A}}));
    }
  }
}

TEST(RaftPersistenceCrashTest,
     HigherTermAndAppendTransitionIsSafeAtEveryBoundary) {
  enum class EntryExpectation { absent, optional, present };
  struct Case final {
    const char* point;
    const char* occurrence;
    Term expected_term;
    EntryExpectation entry;
    bool response_visible;
  };
  for (const auto& test_case : {
           Case{"before_persist", "1", 0, EntryExpectation::absent, false},
           Case{"after_write", "1", 0, EntryExpectation::absent, false},
           Case{"after_file_sync", "1", 0, EntryExpectation::absent, false},
           Case{"after_rename", "1", 2, EntryExpectation::absent, false},
           Case{"after_sync", "1", 2, EntryExpectation::absent, false},
           Case{"before_persist", "2", 2, EntryExpectation::absent, false},
           Case{"after_write", "2", 2, EntryExpectation::optional, false},
           Case{"after_file_sync", "2", 2, EntryExpectation::present, false},
           Case{"after_sync", "2", 2, EntryExpectation::present, false},
           Case{"before_response", "1", 2, EntryExpectation::present, false},
           Case{"after_response", "1", 2, EntryExpectation::present, true},
       }) {
    CrashDirectory directory;
    run_helper(directory.path(), "higher_log", test_case.point,
               test_case.occurrence);
    const bool marker =
        std::filesystem::exists(directory.path() / "response.marker");
    EXPECT_EQ(marker, test_case.response_visible) << test_case.point;

    auto storage = open_storage(directory.path());
    EXPECT_EQ(storage.state().current_term, test_case.expected_term)
        << test_case.point << " occurrence " << test_case.occurrence;
    if (test_case.entry == EntryExpectation::absent) {
      EXPECT_TRUE(storage.state().log.empty()) << test_case.point;
    } else if (test_case.entry == EntryExpectation::present || marker) {
      ASSERT_EQ(storage.state().log.size(), 1U) << test_case.point;
      EXPECT_EQ(storage.state().log.front().term, 2U);
    }
  }
}

TEST(RaftPersistenceCrashTest,
     RecoveryDurablyCompletesAVisibleRenamedVoteBeforeReturning) {
  CrashDirectory directory;
  run_helper(directory.path(), "vote", "after_rename", "2");

  // This restart accepts the complete renamed generation, syncs the journal
  // and directory, then is killed immediately. A second restart must retain it.
  run_helper(directory.path(), "recover", "before_persist");
  auto recovered = open_storage(directory.path());
  EXPECT_EQ(recovered.state().current_term, 1U);
  EXPECT_EQ(recovered.state().voted_for, 2U);
}

TEST(RaftPersistenceCrashTest, VisibleVoteCannotBecomeASecondVoteAfterRestart) {
  CrashDirectory directory;
  run_helper(directory.path(), "vote", "after_response");

  std::optional<RequestVoteResponse> response;
  auto restarted = PersistedRaftNode::open(PersistedRaftOptions{
      .config = config(),
      .data_directory = directory.path(),
      .initial_time = 500,
      .output = [&response](const Action& action) {
        const auto* send = std::get_if<SendMessage>(&action);
        if (send != nullptr) {
          if (const auto* vote =
                  std::get_if<RequestVoteResponse>(&send->message)) {
            response = *vote;
          }
        }
      },
      .crash_hook = {},
  });
  restarted.step(3, RequestVote{.term = 1,
                                .candidate_id = 3,
                                .last_log_index = 0,
                                .last_log_term = 0});
  ASSERT_TRUE(response.has_value());
  EXPECT_FALSE(response->vote_granted);
  EXPECT_EQ(restarted.durable_state().voted_for, 2U);
}

}  // namespace
}  // namespace forgekv::raft
