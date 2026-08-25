#include "sim/raft_simulator.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <vector>

namespace forgekv::sim {
namespace {

SimulatorConfig simulator_config(const std::uint64_t seed = 91) {
  return SimulatorConfig{
      .voters = {1, 2, 3},
      .election_timeout_min = 100,
      .election_timeout_max = 200,
      .heartbeat_interval = 25,
      .seed = seed,
      .max_pending_messages = 1'000,
      .max_trace_records = 1'000,
  };
}

raft::LogicalTime earliest_deadline(const RaftSimulator& simulator) {
  auto earliest = std::numeric_limits<raft::LogicalTime>::max();
  for (const raft::NodeId node_id : {1U, 2U, 3U}) {
    const auto view = simulator.node(node_id);
    if (view.active && view.state.has_value()) {
      earliest = std::min(earliest, view.state->election_deadline);
    }
  }
  return earliest;
}

std::optional<raft::NodeId> leader(const RaftSimulator& simulator) {
  for (const raft::NodeId node_id : {1U, 2U, 3U}) {
    const auto view = simulator.node(node_id);
    if (view.state.has_value() && view.state->role == raft::Role::leader) {
      return node_id;
    }
  }
  return std::nullopt;
}

raft::NodeId elect_leader(RaftSimulator& simulator) {
  for (std::size_t attempt = 0; attempt < 20; ++attempt) {
    if (const auto elected = leader(simulator); elected.has_value()) {
      return *elected;
    }
    simulator.advance_time_to(earliest_deadline(simulator));
    simulator.deliver_all();
  }
  throw std::runtime_error("simulator did not elect a leader");
}

TEST(RaftSimulatorControls, StartsThreeDeterministicFollowers) {
  RaftSimulator simulator(simulator_config());
  EXPECT_EQ(simulator.now(), 0U);
  EXPECT_EQ(simulator.seed(), 91U);
  EXPECT_TRUE(simulator.pending_messages().empty());

  for (const raft::NodeId node_id : {1U, 2U, 3U}) {
    const auto view = simulator.node(node_id);
    ASSERT_TRUE(view.active);
    ASSERT_TRUE(view.state.has_value());
    EXPECT_EQ(view.state->role, raft::Role::follower);
    EXPECT_EQ(view.state->current_term, 0U);
    EXPECT_TRUE(view.persistent.log.empty());
  }
}

TEST(RaftSimulatorControls, LogicalElectionSchedulesMonotonicMessageIds) {
  RaftSimulator simulator(simulator_config());
  const auto deadline = earliest_deadline(simulator);
  simulator.advance_time_to(deadline);

  const auto pending = simulator.pending_messages();
  ASSERT_GE(pending.size(), 2U);
  for (std::size_t index = 1; index < pending.size(); ++index) {
    EXPECT_LT(pending[index - 1].id, pending[index].id);
  }
  EXPECT_EQ(simulator.now(), deadline);
  EXPECT_EQ(simulator.operation_count(), 1U);
}

TEST(RaftSimulatorControls, DelayDropAndSelectedDeliveryAreExplicit) {
  RaftSimulator simulator(simulator_config());
  simulator.advance_time_to(earliest_deadline(simulator));
  auto pending = simulator.pending_messages();
  ASSERT_GE(pending.size(), 2U);
  const auto delayed_id = pending[0].id;
  const auto dropped_id = pending[1].id;

  simulator.delay(delayed_id, 50);
  simulator.drop(dropped_id);
  EXPECT_FALSE(simulator.deliver(delayed_id));
  simulator.advance_time_to(simulator.now() + 50);
  EXPECT_TRUE(simulator.deliver(delayed_id));

  pending = simulator.pending_messages();
  EXPECT_EQ(std::ranges::find_if(
                pending, [dropped_id](const ScheduledMessage& message) {
                  return message.id == dropped_id;
                }),
            pending.end());
}

TEST(RaftSimulatorControls, DelayStartsAtCurrentTimeForAnOldMessage) {
  RaftSimulator simulator(simulator_config());
  simulator.advance_time_to(earliest_deadline(simulator));
  const auto delayed_id = simulator.pending_messages().front().id;
  simulator.advance_time_to(simulator.now() + 500);

  simulator.delay(delayed_id, 50);
  const auto pending = simulator.pending_messages();
  const auto delayed = std::ranges::find_if(
      pending, [delayed_id](const ScheduledMessage& message) {
        return message.id == delayed_id;
      });
  ASSERT_NE(delayed, pending.end());
  EXPECT_EQ(delayed->deliver_at, simulator.now() + 50);
  EXPECT_FALSE(simulator.deliver(delayed_id));
}

TEST(RaftSimulatorControls, BoundsCoverLargestAtomicElectionBurst) {
  auto config = simulator_config();
  config.voters = {1, 2, 3, 4, 5, 6, 7};
  config.election_timeout_min = 100;
  config.election_timeout_max = 100;
  config.max_pending_messages = 41;
  EXPECT_THROW(
      {
        RaftSimulator undersized(config);
      },
      std::invalid_argument);

  config.max_pending_messages = 42;
  RaftSimulator simulator(config);
  EXPECT_NO_THROW(simulator.advance_time_to(100));
  EXPECT_EQ(simulator.pending_messages().size(), 42U);
}

TEST(RaftSimulatorControls, PartitionBlocksExistingMessageUntilHeal) {
  RaftSimulator simulator(simulator_config());
  simulator.advance_time_to(earliest_deadline(simulator));
  const auto pending = simulator.pending_messages();
  ASSERT_FALSE(pending.empty());
  const auto message = pending.front();

  simulator.block_link(message.from, message.to);
  EXPECT_FALSE(simulator.deliver(message.id));
  simulator.heal_link(message.from, message.to);
  EXPECT_TRUE(simulator.deliver(message.id));
}

TEST(RaftSimulatorFailures, CrashRestartRestoresOnlyDurableRaftState) {
  RaftSimulator simulator(simulator_config(711));
  const auto elected = elect_leader(simulator);
  simulator.propose(elected, {std::byte{0x41}, std::byte{0x42}});
  simulator.deliver_all();

  const raft::NodeId follower = elected == 1 ? 2 : 1;
  const auto before = simulator.node(follower);
  ASSERT_TRUE(before.state.has_value());
  ASSERT_FALSE(before.persistent.log.empty());
  simulator.crash(follower);
  EXPECT_FALSE(simulator.node(follower).active);
  simulator.advance_time_to(simulator.now() + 17);
  simulator.restart(follower);

  const auto after = simulator.node(follower);
  ASSERT_TRUE(after.state.has_value());
  EXPECT_EQ(after.persistent.current_term, before.persistent.current_term);
  EXPECT_EQ(after.persistent.voted_for, before.persistent.voted_for);
  EXPECT_EQ(after.persistent.log, before.persistent.log);
  EXPECT_EQ(after.state->commit_index, 0U);
  EXPECT_EQ(after.state->last_applied, 0U);
  EXPECT_EQ(after.state->now, simulator.now());
  EXPECT_GT(after.state->election_deadline, simulator.now());
}

TEST(RaftSimulatorFailures, MajorityElectsNewLeaderAcrossOldLeaderPartition) {
  RaftSimulator simulator(simulator_config(991));
  const auto old_leader = elect_leader(simulator);
  const auto old_term = simulator.node(old_leader).state->current_term;
  simulator.isolate(old_leader);

  std::optional<raft::NodeId> replacement;
  for (std::size_t attempt = 0; attempt < 20 && !replacement.has_value();
       ++attempt) {
    simulator.advance_time_to(earliest_deadline(simulator));
    simulator.deliver_all();
    for (const raft::NodeId node_id : {1U, 2U, 3U}) {
      if (node_id != old_leader && simulator.node(node_id).state->role ==
                                       raft::Role::leader) {
        replacement = node_id;
      }
    }
  }
  ASSERT_TRUE(replacement.has_value()) << simulator.dump();
  EXPECT_GT(simulator.node(*replacement).state->current_term, old_term);
  EXPECT_EQ(simulator.node(old_leader).state->role, raft::Role::leader);

  simulator.heal_all();
  simulator.deliver_all();
  EXPECT_NE(simulator.node(old_leader).state->role, raft::Role::leader);
  simulator.check_invariants();
}

TEST(RaftSimulatorFailures, DumpContainsReplayAndSafetyEvidence) {
  RaftSimulator simulator(simulator_config(1717));
  const auto elected = elect_leader(simulator);
  simulator.propose(elected, {std::byte{0x7A}});
  simulator.deliver_all();
  simulator.block_link(1, 2);

  const auto report = simulator.dump();
  EXPECT_NE(report.find("seed=1717 operations="), std::string::npos);
  EXPECT_NE(report.find("blocked-links 1<->2"), std::string::npos);
  EXPECT_NE(report.find("elected-leaders term="), std::string::npos);
  EXPECT_NE(report.find("committed-history 1:"), std::string::npos);
  EXPECT_NE(report.find("applied-history"), std::string::npos);
  EXPECT_NE(report.find("AppendEntries(term="), std::string::npos);
  EXPECT_NE(report.find("previous_index="), std::string::npos);
}

TEST(RaftSimulatorRandom, SameSeedProducesIdenticalTraceAndStateDump) {
  RaftSimulator first(simulator_config(0xC0FFEE));
  RaftSimulator second(simulator_config(0xC0FFEE));
  first.run_random(2'000);
  second.run_random(2'000);
  EXPECT_EQ(first.trace(), second.trace());
  EXPECT_EQ(first.dump(), second.dump());
  EXPECT_GE(first.operation_count(), 2'000U);
}

TEST(RaftSimulatorRandom, FiftyThousandSeededOperationsPreserveSafety) {
  for (std::uint64_t seed = 1; seed <= 50; ++seed) {
    RaftSimulator simulator(simulator_config(seed));
    try {
      simulator.run_random(1'000);
      simulator.check_invariants();
    } catch (const std::exception& error) {
      ADD_FAILURE() << "seed=" << seed << " error=" << error.what()
                    << '\n' << simulator.dump();
      return;
    }
  }
}

TEST(RaftSimulatorRandom, FiveAndSevenNodeHistoriesRespectBoundsAndSafety) {
  for (const auto& voters :
       {std::vector<raft::NodeId>{1, 2, 3, 4, 5},
        std::vector<raft::NodeId>{1, 2, 3, 4, 5, 6, 7}}) {
    auto config = simulator_config(8821 + voters.size());
    config.voters = voters;
    config.max_pending_messages = 2'000;
    RaftSimulator simulator(config);
    EXPECT_NO_THROW(simulator.run_random(5'000)) << simulator.dump();
    EXPECT_LE(simulator.pending_messages().size(),
              config.max_pending_messages);
  }
}

}  // namespace
}  // namespace forgekv::sim
