#include "raft/persisted_raft_node.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>

#include <unistd.h>

namespace forgekv::raft {
namespace {

class DriverDirectory final {
 public:
  DriverDirectory() {
    static std::atomic<std::uint64_t> sequence{0};
    path_ = std::filesystem::temp_directory_path() /
            ("forgekv-raft-driver-" + std::to_string(::getpid()) + "-" +
             std::to_string(sequence.fetch_add(1)));
    std::filesystem::create_directories(path_);
  }
  ~DriverDirectory() { std::filesystem::remove_all(path_); }
  [[nodiscard]] const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

RaftConfig driver_config() {
  return RaftConfig{
      .self_id = 1,
      .voters = {1, 2, 3},
      .election_timeout_min = 100,
      .election_timeout_max = 200,
      .heartbeat_interval = 25,
      .random_seed = 765,
  };
}

std::string point_name(const RaftCrashPoint point) {
  switch (point) {
    case RaftCrashPoint::before_persist:
      return "before_persist";
    case RaftCrashPoint::after_write:
      return "after_write";
    case RaftCrashPoint::after_file_sync:
      return "after_file_sync";
    case RaftCrashPoint::after_rename:
      return "after_rename";
    case RaftCrashPoint::after_sync:
      return "after_sync";
    case RaftCrashPoint::before_response:
      return "before_response";
    case RaftCrashPoint::after_response:
      return "after_response";
  }
  return "unknown";
}

struct StatefulCountingHook final {
  void operator()(const RaftCrashPoint) {
    observations->push_back(++calls);
  }

  std::vector<std::size_t>* observations{};
  std::size_t calls{};
};

TEST(PersistedRaftNodeTest, VoteGrantIsPublishedOnlyAfterDurableSync) {
  DriverDirectory directory;
  std::vector<std::string> events;
  auto node = PersistedRaftNode::open(PersistedRaftOptions{
      .config = driver_config(),
      .data_directory = directory.path(),
      .initial_time = 0,
      .output = [&events](const Action& action) {
        EXPECT_EQ(std::get_if<PersistHardState>(&action), nullptr);
        EXPECT_EQ(std::get_if<PersistLog>(&action), nullptr);
        events.push_back("output");
      },
      .crash_hook = [&events](const RaftCrashPoint point) {
        events.push_back(point_name(point));
      },
  });

  node.step(2, RequestVote{
                   .term = 1,
                   .candidate_id = 2,
                   .last_log_index = 0,
                   .last_log_term = 0,
               });

  EXPECT_EQ(events,
            (std::vector<std::string>{
                "before_persist", "after_write", "after_file_sync",
                "after_rename", "after_sync", "before_persist",
                "after_write", "after_file_sync", "after_rename", "after_sync",
                "before_response", "output", "after_response"}));
  EXPECT_EQ(node.durable_state().current_term, 1U);
  EXPECT_EQ(node.durable_state().voted_for, 2U);
}

TEST(PersistedRaftNodeTest, RestartRejectsChangedFixedMembership) {
  DriverDirectory directory;
  {
    auto node = PersistedRaftNode::open(PersistedRaftOptions{
        .config = driver_config(),
        .data_directory = directory.path(),
        .initial_time = 0,
        .output = [](const Action&) {},
        .crash_hook = {},
    });
    EXPECT_FALSE(node.failed());
  }

  auto changed = driver_config();
  changed.voters = {1, 2, 4};
  EXPECT_THROW(
      static_cast<void>(PersistedRaftNode::open(PersistedRaftOptions{
          .config = changed,
          .data_directory = directory.path(),
          .initial_time = 0,
          .output = [](const Action&) {},
          .crash_hook = {},
      })),
      RaftStorageError);
}

TEST(PersistedRaftNodeTest, InvalidConfigCannotInitializeStorage) {
  DriverDirectory directory;
  auto invalid = driver_config();
  invalid.voters = {1, 2, 3, 4, 5, 6, 7, 8, 9};
  EXPECT_THROW(
      static_cast<void>(PersistedRaftNode::open(PersistedRaftOptions{
          .config = invalid,
          .data_directory = directory.path(),
          .initial_time = 0,
          .output = [](const Action&) {},
          .crash_hook = {},
      })),
      std::invalid_argument);
  EXPECT_TRUE(std::filesystem::is_empty(directory.path()));

  auto node = PersistedRaftNode::open(PersistedRaftOptions{
      .config = driver_config(),
      .data_directory = directory.path(),
      .initial_time = 0,
      .output = [](const Action&) {},
      .crash_hook = {},
  });
  EXPECT_FALSE(node.failed());
}

TEST(PersistedRaftNodeTest, StorageAndDriverShareOneStatefulCrashHook) {
  DriverDirectory directory;
  std::vector<std::size_t> observations;
  auto node = PersistedRaftNode::open(PersistedRaftOptions{
      .config = driver_config(),
      .data_directory = directory.path(),
      .initial_time = 0,
      .output = [](const Action&) {},
      .crash_hook = StatefulCountingHook{.observations = &observations},
  });

  node.step(2, RequestVote{
                   .term = 1,
                   .candidate_id = 2,
                   .last_log_index = 0,
                   .last_log_term = 0,
               });

  EXPECT_EQ(observations,
            (std::vector<std::size_t>{1, 2, 3, 4, 5, 6,
                                      7, 8, 9, 10, 11, 12}));
}

TEST(PersistedRaftNodeTest, AppendResponseObservesDurableHardStateAndLog) {
  DriverDirectory directory;
  const auto path = directory.path();
  bool checked_response = false;
  PersistedRaftNode* live_node = nullptr;
  auto node = PersistedRaftNode::open(PersistedRaftOptions{
      .config = driver_config(),
      .data_directory = path,
      .initial_time = 0,
      .output = [&live_node, &checked_response](const Action& action) {
        const auto* send = std::get_if<SendMessage>(&action);
        if (send == nullptr) {
          return;
        }
        const auto* response =
            std::get_if<AppendEntriesResponse>(&send->message);
        ASSERT_NE(response, nullptr);
        ASSERT_TRUE(response->success);
        ASSERT_NE(live_node, nullptr);
        const auto disk = live_node->durable_state();
        EXPECT_EQ(disk.current_term, 2U);
        ASSERT_EQ(disk.log.size(), 1U);
        EXPECT_EQ(disk.log.front().term, 2U);
        checked_response = true;
      },
      .crash_hook = {},
  });
  live_node = &node;

  node.step(2, AppendEntries{
                   .term = 2,
                   .leader_id = 2,
                   .previous_log_index = 0,
                   .previous_log_term = 0,
                   .entries = {LogEntry{.index = 1,
                                        .term = 2,
                                        .kind = EntryKind::command,
                                        .command = {std::byte{0x31}}}},
                   .leader_commit = 0,
                   .rpc_id = 9,
               });
  EXPECT_TRUE(checked_response);
}

TEST(PersistedRaftNodeTest, ElectionAndProposalPersistBeforeReplication) {
  DriverDirectory directory;
  std::vector<Action> output;
  auto node = PersistedRaftNode::open(PersistedRaftOptions{
      .config = driver_config(),
      .data_directory = directory.path(),
      .initial_time = 0,
      .output = [&output](const Action& action) { output.push_back(action); },
      .crash_hook = {},
  });
  node.advance_time(node.snapshot().election_deadline);
  node.step(2, RequestVoteResponse{.term = 1, .vote_granted = true});
  output.clear();

  node.propose({std::byte{0x55}});
  ASSERT_FALSE(output.empty());
  const auto durable = node.durable_state();
  ASSERT_EQ(durable.log.size(), 2U);
  EXPECT_EQ(durable.log.back().command,
            (std::vector<std::byte>{std::byte{0x55}}));
  for (const auto& action : output) {
    EXPECT_EQ(std::get_if<PersistHardState>(&action), nullptr);
    EXPECT_EQ(std::get_if<PersistLog>(&action), nullptr);
  }
}

TEST(PersistedRaftNodeTest, PersistenceHookFailureFaultsAndSuppressesDriver) {
  DriverDirectory directory;
  std::size_t outputs = 0;
  auto node = PersistedRaftNode::open(PersistedRaftOptions{
      .config = driver_config(),
      .data_directory = directory.path(),
      .initial_time = 0,
      .output = [&outputs](const Action&) { ++outputs; },
      .crash_hook = [](const RaftCrashPoint point) {
        if (point == RaftCrashPoint::after_write) {
          throw std::runtime_error("injected crash");
        }
      },
  });

  EXPECT_THROW(node.step(2, RequestVote{
                                .term = 1,
                                .candidate_id = 2,
                                .last_log_index = 0,
                                .last_log_term = 0,
                            }),
               std::runtime_error);
  EXPECT_TRUE(node.failed());
  EXPECT_EQ(outputs, 0U);
  EXPECT_THROW(node.advance_time(500), std::logic_error);
}

TEST(PersistedRaftNodeTest, RestartRestoresDurableCoreState) {
  DriverDirectory directory;
  {
    auto node = PersistedRaftNode::open(PersistedRaftOptions{
        .config = driver_config(),
        .data_directory = directory.path(),
        .initial_time = 0,
        .output = [](const Action&) {},
        .crash_hook = {},
    });
    node.step(2, RequestVote{
                     .term = 4,
                     .candidate_id = 2,
                     .last_log_index = 0,
                     .last_log_term = 0,
                 });
  }

  auto restarted = PersistedRaftNode::open(PersistedRaftOptions{
      .config = driver_config(),
      .data_directory = directory.path(),
      .initial_time = 900,
      .output = [](const Action&) {},
      .crash_hook = {},
  });
  EXPECT_EQ(restarted.snapshot().current_term, 4U);
  EXPECT_EQ(restarted.snapshot().voted_for, 2U);
  EXPECT_EQ(restarted.snapshot().now, 900U);
  EXPECT_FALSE(restarted.failed());
}

}  // namespace
}  // namespace forgekv::raft
