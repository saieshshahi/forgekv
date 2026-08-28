#include "raft/raft_node.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <variant>
#include <vector>

namespace forgekv::raft {
namespace {

RaftConfig config(const NodeId self_id = 1, const std::uint64_t seed = 7) {
  return RaftConfig{
      .self_id = self_id,
      .voters = {1, 2, 3},
      .election_timeout_min = 150,
      .election_timeout_max = 300,
      .heartbeat_interval = 50,
      .random_seed = seed,
  };
}

RaftConfig five_node_config(const NodeId self_id = 1,
                            const std::uint64_t seed = 7) {
  auto result = config(self_id, seed);
  result.voters = {1, 2, 3, 4, 5};
  return result;
}

Actions trigger_election(RaftNode& node) {
  return node.advance_time(node.snapshot().election_deadline);
}

LogEntry entry(const LogIndex index, const Term term,
               const std::uint8_t value) {
  return LogEntry{
      .index = index,
      .term = term,
      .kind = EntryKind::command,
      .command = {static_cast<std::byte>(value)},
  };
}

AppendEntries append(const Term term, const NodeId leader_id,
                     const LogIndex previous_index,
                     const Term previous_term, std::vector<LogEntry> entries,
                     const LogIndex leader_commit, const RpcId rpc_id) {
  return AppendEntries{
      .term = term,
      .leader_id = leader_id,
      .previous_log_index = previous_index,
      .previous_log_term = previous_term,
      .entries = std::move(entries),
      .leader_commit = leader_commit,
      .rpc_id = rpc_id,
  };
}

Actions elect(RaftNode& node) {
  static_cast<void>(trigger_election(node));
  return node.step(2, RequestVoteResponse{
                          .term = node.snapshot().current_term,
                          .vote_granted = true,
                      });
}

template <typename T>
std::vector<T> actions_of(const Actions& actions) {
  std::vector<T> result;
  for (const auto& action : actions) {
    if (const auto* value = std::get_if<T>(&action)) {
      result.push_back(*value);
    }
  }
  return result;
}

TEST(RaftNodeConfig, RejectsInvalidMembershipAndTiming) {
  auto missing_self = config();
  missing_self.voters = {2, 3, 4};
  EXPECT_THROW(RaftNode node(missing_self), std::invalid_argument);

  auto duplicate = config();
  duplicate.voters = {1, 2, 2};
  EXPECT_THROW(RaftNode node(duplicate), std::invalid_argument);

  auto heartbeat_too_slow = config();
  heartbeat_too_slow.heartbeat_interval =
      heartbeat_too_slow.election_timeout_min;
  EXPECT_THROW(RaftNode node(heartbeat_too_slow), std::invalid_argument);
}

TEST(RaftElection, TerminalTermCannotWrapOnElection) {
  RaftNode node(config(),
                RaftPersistentState{
                    .current_term = std::numeric_limits<Term>::max(),
                    .voted_for = std::nullopt,
                    .snapshot = std::nullopt,
                    .log = {},
                },
                0);
  EXPECT_THROW(static_cast<void>(trigger_election(node)), std::overflow_error);
  EXPECT_EQ(node.snapshot().current_term, std::numeric_limits<Term>::max());
}

TEST(RaftNodeRecovery, RestoresPersistentStateAtRestartLogicalTime) {
  const RaftPersistentState persistent{
      .current_term = 7,
      .voted_for = 2,
      .snapshot = std::nullopt,
      .log = {entry(1, 4, 0x41), entry(2, 7, 0x72)},
  };
  RaftNode node(config(), persistent, 1'000);
  const auto state = node.snapshot();

  EXPECT_EQ(state.role, Role::follower);
  EXPECT_EQ(state.current_term, 7U);
  EXPECT_EQ(state.voted_for, 2U);
  EXPECT_EQ(state.log, persistent.log);
  EXPECT_EQ(state.commit_index, 0U);
  EXPECT_EQ(state.last_applied, 0U);
  EXPECT_EQ(state.now, 1'000U);
  EXPECT_GE(state.election_deadline, 1'150U);
  EXPECT_LE(state.election_deadline, 1'300U);
}

TEST(RaftNodeRecovery, RestoresCompactedSnapshotBoundaryAndSuffix) {
  const StateMachineSnapshot durable{
      .last_included_index = 5,
      .last_included_term = 2,
      .state_machine = {std::byte{0xAA}},
  };
  const RaftPersistentState persistent{
      .current_term = 3,
      .voted_for = std::nullopt,
      .snapshot = durable,
      .log = {entry(6, 2, 0x61), entry(7, 3, 0x72)},
  };
  RaftNode node(config(), persistent, 100);

  const auto state = node.snapshot();
  EXPECT_EQ(state.durable_snapshot, durable);
  EXPECT_EQ(state.log, persistent.log);
  EXPECT_EQ(state.commit_index, 5U);
  EXPECT_EQ(state.last_applied, 5U);

  const auto election = trigger_election(node);
  const auto sends = actions_of<SendMessage>(election);
  ASSERT_FALSE(sends.empty());
  const auto* vote = std::get_if<RequestVote>(&sends.front().message);
  ASSERT_NE(vote, nullptr);
  EXPECT_EQ(vote->last_log_index, 7U);
  EXPECT_EQ(vote->last_log_term, 3U);
}

TEST(RaftNodeRecovery, RejectsSnapshotBoundaryWithoutRepresentableSuccessor) {
  EXPECT_THROW(
      RaftNode node(
          config(),
          RaftPersistentState{
              .current_term = 3,
              .voted_for = std::nullopt,
              .snapshot = StateMachineSnapshot{
                  .last_included_index =
                      std::numeric_limits<LogIndex>::max(),
                  .last_included_term = 2,
                  .state_machine = {std::byte{0xFF}},
              },
              .log = {},
          },
          0),
      std::invalid_argument);
}

TEST(RaftSnapshotCompaction, RetainsOnlySuffixAfterAppliedBoundary) {
  RaftNode node(config());
  static_cast<void>(node.step(
      2, append(2, 2, 0, 0, {entry(1, 2, 0x11), entry(2, 2, 0x22)}, 2,
                1)));
  ASSERT_EQ(node.snapshot().last_applied, 2U);

  const StateMachineSnapshot compacted{
      .last_included_index = 2,
      .last_included_term = 2,
      .state_machine = {std::byte{0xCA}, std::byte{0xFE}},
  };
  const auto actions = node.compact(compacted);
  ASSERT_EQ(actions_of<PersistSnapshot>(actions).size(), 1U);
  EXPECT_EQ(actions_of<PersistSnapshot>(actions).front().snapshot, compacted);
  EXPECT_EQ(node.snapshot().durable_snapshot, compacted);
  EXPECT_TRUE(node.snapshot().log.empty());
  EXPECT_EQ(node.snapshot().commit_index, 2U);
  EXPECT_EQ(node.snapshot().last_applied, 2U);

  EXPECT_THROW(node.compact(compacted), std::invalid_argument);
  auto wrong_term = compacted;
  wrong_term.last_included_index = 1;
  wrong_term.last_included_term = 1;
  EXPECT_THROW(node.compact(std::move(wrong_term)), std::invalid_argument);
}

TEST(RaftSnapshotInstallation, FollowerAssemblesChunksBeforePersistAndApply) {
  RaftNode follower(config());
  const std::vector<std::byte> image{std::byte{0x10}, std::byte{0x20},
                                     std::byte{0x30}};
  auto actions = follower.step(
      2, InstallSnapshot{.term = 3,
                         .leader_id = 2,
                         .last_included_index = 5,
                         .last_included_term = 2,
                         .total_size = image.size(),
                         .offset = 0,
                         .data = {image.begin(), image.begin() + 2},
                         .done = false,
                         .rpc_id = 11});
  EXPECT_TRUE(actions_of<PersistSnapshot>(actions).empty());
  auto replies = actions_of<SendMessage>(actions);
  ASSERT_EQ(replies.size(), 1U);
  EXPECT_EQ(std::get<InstallSnapshotResponse>(replies[0].message).next_offset,
            2U);

  actions = follower.step(
      2, InstallSnapshot{.term = 3,
                         .leader_id = 2,
                         .last_included_index = 5,
                         .last_included_term = 2,
                         .total_size = image.size(),
                         .offset = 2,
                         .data = {image.begin() + 2, image.end()},
                         .done = true,
                         .rpc_id = 12});
  ASSERT_EQ(actions_of<PersistSnapshot>(actions).size(), 1U);
  ASSERT_EQ(actions_of<ApplySnapshot>(actions).size(), 1U);
  EXPECT_EQ(actions_of<ApplySnapshot>(actions)[0].snapshot.state_machine,
            image);
  EXPECT_EQ(follower.snapshot().commit_index, 5U);
  EXPECT_EQ(follower.snapshot().last_applied, 5U);
  EXPECT_EQ(follower.snapshot().durable_snapshot->state_machine, image);
}

TEST(RaftSnapshotInstallation,
     RejectsSnapshotBoundaryWithoutRepresentableSuccessor) {
  RaftNode follower(config());
  const auto actions = follower.step(
      2, InstallSnapshot{
             .term = 3,
             .leader_id = 2,
             .last_included_index = std::numeric_limits<LogIndex>::max(),
             .last_included_term = 2,
             .total_size = 1,
             .offset = 0,
             .data = {std::byte{0xFF}},
             .done = true,
             .rpc_id = 11,
         });
  EXPECT_TRUE(actions_of<PersistSnapshot>(actions).empty());
  EXPECT_TRUE(actions_of<ApplySnapshot>(actions).empty());
  const auto replies = actions_of<SendMessage>(actions);
  ASSERT_EQ(replies.size(), 1U);
  EXPECT_FALSE(std::get<InstallSnapshotResponse>(replies[0].message).success);
}

TEST(RaftSnapshotInstallation, LeaderSendsSnapshotAfterFollowerRejectsBelowBase) {
  const StateMachineSnapshot snapshot{
      .last_included_index = 2,
      .last_included_term = 2,
      .state_machine = {std::byte{0xAB}},
  };
  RaftNode leader(config(),
                  RaftPersistentState{.current_term = 2,
                                      .voted_for = std::nullopt,
                                      .snapshot = snapshot,
                                      .log = {}},
                  0);
  const auto elected = elect(leader);
  const auto sends = actions_of<SendMessage>(elected);
  const auto to_two = std::ranges::find_if(
      sends, [](const SendMessage& send) { return send.to == 2; });
  ASSERT_NE(to_two, sends.end());
  const auto rpc = std::get<AppendEntries>(to_two->message).rpc_id;

  const auto retry = leader.step(
      2, AppendEntriesResponse{.term = 3,
                               .success = false,
                               .match_index = 0,
                               .reject_hint = 1,
                               .rpc_id = rpc});
  const auto retry_sends = actions_of<SendMessage>(retry);
  ASSERT_EQ(retry_sends.size(), 1U);
  const auto* install =
      std::get_if<InstallSnapshot>(&retry_sends.front().message);
  ASSERT_NE(install, nullptr);
  EXPECT_EQ(install->last_included_index, 2U);
  EXPECT_EQ(install->data, snapshot.state_machine);
  EXPECT_TRUE(install->done);
}

TEST(RaftSnapshotInstallation, ResponsesCannotSkipChunksAndNewImageRestartsAtZero) {
  constexpr std::size_t chunk_size = 1024U * 1024U;
  const StateMachineSnapshot snapshot{
      .last_included_index = 2,
      .last_included_term = 2,
      .state_machine = std::vector<std::byte>(chunk_size + 7U,
                                               std::byte{0xAB}),
  };
  RaftNode leader(config(),
                  RaftPersistentState{.current_term = 2,
                                      .voted_for = std::nullopt,
                                      .snapshot = snapshot,
                                      .log = {}},
                  0);
  const auto elected = elect(leader);
  const auto sends = actions_of<SendMessage>(elected);
  const auto to_two = std::ranges::find_if(
      sends, [](const SendMessage& send) { return send.to == 2; });
  const auto to_three = std::ranges::find_if(
      sends, [](const SendMessage& send) { return send.to == 3; });
  ASSERT_NE(to_two, sends.end());
  ASSERT_NE(to_three, sends.end());
  const auto two_rpc = std::get<AppendEntries>(to_two->message).rpc_id;
  const auto three_append = std::get<AppendEntries>(to_three->message);
  auto actions = leader.step(
      2, AppendEntriesResponse{.term = 3,
                               .success = false,
                               .match_index = 0,
                               .reject_hint = 1,
                               .rpc_id = two_rpc});
  auto installs = actions_of<SendMessage>(actions);
  ASSERT_EQ(installs.size(), 1U);
  const auto first = std::get<InstallSnapshot>(installs[0].message);
  ASSERT_EQ(first.offset, 0U);
  ASSERT_EQ(first.data.size(), chunk_size);
  ASSERT_FALSE(first.done);

  actions = leader.step(
      2, InstallSnapshotResponse{.term = 3,
                                 .success = true,
                                 .last_included_index = 2,
                                 .next_offset = snapshot.state_machine.size(),
                                 .rpc_id = first.rpc_id});
  EXPECT_TRUE(actions.empty());
  EXPECT_EQ(leader.progress(2)->snapshot_offset, 0U);

  actions = leader.step(
      2, InstallSnapshotResponse{.term = 3,
                                 .success = true,
                                 .last_included_index = 2,
                                 .next_offset = chunk_size,
                                 .rpc_id = first.rpc_id});
  installs = actions_of<SendMessage>(actions);
  ASSERT_EQ(installs.size(), 1U);
  EXPECT_EQ(std::get<InstallSnapshot>(installs[0].message).offset, chunk_size);

  static_cast<void>(leader.step(
      3, AppendEntriesResponse{.term = 3,
                               .success = true,
                               .match_index = 3,
                               .reject_hint = 0,
                               .rpc_id = three_append.rpc_id}));

  const StateMachineSnapshot newer{
      .last_included_index = 3,
      .last_included_term = 3,
      .state_machine = {std::byte{0xCC}},
  };
  static_cast<void>(leader.compact(newer));
  actions = leader.advance_time(leader.snapshot().heartbeat_deadline);
  installs = actions_of<SendMessage>(actions);
  const auto new_to_two = std::ranges::find_if(
      installs, [](const SendMessage& send) { return send.to == 2; });
  ASSERT_NE(new_to_two, installs.end());
  const auto& restarted = std::get<InstallSnapshot>(new_to_two->message);
  EXPECT_EQ(restarted.last_included_index, 3U);
  EXPECT_EQ(restarted.offset, 0U);
}

TEST(RaftNodeRecovery, RejectsInvalidPersistentState) {
  auto persistent = RaftPersistentState{
      .current_term = 2,
      .voted_for = 99,
      .snapshot = std::nullopt,
      .log = {},
  };
  EXPECT_THROW(RaftNode node(config(), persistent, 0), std::invalid_argument);

  persistent.voted_for.reset();
  persistent.log = {entry(2, 2, 0x22)};
  EXPECT_THROW(RaftNode node(config(), persistent, 0), std::invalid_argument);

  persistent.log = {entry(1, 3, 0x31)};
  EXPECT_THROW(RaftNode node(config(), persistent, 0), std::invalid_argument);

  persistent = RaftPersistentState{
      .current_term = 0,
      .voted_for = 1,
      .snapshot = std::nullopt,
      .log = {},
  };
  EXPECT_THROW(RaftNode node(config(), persistent, 0), std::invalid_argument);
}

TEST(RaftElection, LogicalTimeoutStartsElectionWithPersistenceBeforeMessages) {
  RaftNode node(config());
  const auto initial = node.snapshot();
  ASSERT_GE(initial.election_deadline, 150U);
  ASSERT_LE(initial.election_deadline, 300U);

  EXPECT_TRUE(node.advance_time(initial.election_deadline - 1).empty());
  const auto actions = node.advance_time(initial.election_deadline);
  const auto state = node.snapshot();

  EXPECT_EQ(state.role, Role::candidate);
  EXPECT_EQ(state.current_term, 1U);
  EXPECT_EQ(state.voted_for, 1U);
  ASSERT_GE(actions.size(), 4U);

  const auto* role = std::get_if<RoleChanged>(&actions[0]);
  ASSERT_NE(role, nullptr);
  EXPECT_EQ(role->from, Role::follower);
  EXPECT_EQ(role->to, Role::candidate);
  EXPECT_EQ(role->term, 1U);

  const auto* persist = std::get_if<PersistHardState>(&actions[1]);
  ASSERT_NE(persist, nullptr);
  EXPECT_EQ(persist->term, 1U);
  EXPECT_EQ(persist->voted_for, 1U);

  const auto sends = actions_of<SendMessage>(actions);
  ASSERT_EQ(sends.size(), 2U);
  for (const auto& send : sends) {
    const auto* request = std::get_if<RequestVote>(&send.message);
    ASSERT_NE(request, nullptr);
    EXPECT_EQ(request->term, 1U);
    EXPECT_EQ(request->candidate_id, 1U);
    EXPECT_EQ(request->last_log_index, 0U);
    EXPECT_EQ(request->last_log_term, 0U);
  }
}

TEST(RaftElection, LogicalTimeCannotMoveBackward) {
  RaftNode node(config());
  EXPECT_TRUE(node.advance_time(10).empty());
  EXPECT_THROW(static_cast<void>(node.advance_time(9)), std::invalid_argument);
}

TEST(RaftElection, DuplicateVoteResponseCannotCreateAQuorum) {
  RaftNode node(five_node_config());
  static_cast<void>(trigger_election(node));

  static_cast<void>(node.step(2, RequestVoteResponse{
                                      .term = 1,
                                      .vote_granted = true,
                                  }));
  EXPECT_EQ(node.snapshot().role, Role::candidate);

  static_cast<void>(node.step(2, RequestVoteResponse{
                                      .term = 1,
                                      .vote_granted = true,
                                  }));
  EXPECT_EQ(node.snapshot().role, Role::candidate);

  const auto elected = node.step(3, RequestVoteResponse{
                                        .term = 1,
                                        .vote_granted = true,
                                    });
  EXPECT_EQ(node.snapshot().role, Role::leader);
  EXPECT_EQ(actions_of<RoleChanged>(elected).size(), 1U);
}

TEST(RaftElection, SplitVoteTimesOutIntoAReelection) {
  RaftNode node(five_node_config());
  static_cast<void>(trigger_election(node));
  for (const NodeId peer : {2U, 3U, 4U, 5U}) {
    static_cast<void>(node.step(peer, RequestVoteResponse{
                                         .term = 1,
                                         .vote_granted = false,
                                     }));
  }
  ASSERT_EQ(node.snapshot().role, Role::candidate);

  const auto actions = trigger_election(node);
  const auto state = node.snapshot();
  EXPECT_EQ(state.role, Role::candidate);
  EXPECT_EQ(state.current_term, 2U);
  EXPECT_EQ(state.voted_for, 1U);
  EXPECT_TRUE(actions_of<RoleChanged>(actions).empty());
  ASSERT_EQ(actions_of<PersistHardState>(actions).size(), 1U);
  EXPECT_EQ(actions_of<SendMessage>(actions).size(), 4U);
}

TEST(RaftElection, SimultaneousCandidatesSplitVotesThenOneWinsNextTerm) {
  RaftNode first(five_node_config(1, 21));
  RaftNode second(five_node_config(2, 22));
  RaftNode third(five_node_config(3, 23));
  RaftNode fourth(five_node_config(4, 24));

  const auto first_requests = actions_of<SendMessage>(trigger_election(first));
  const auto second_requests = actions_of<SendMessage>(trigger_election(second));
  const auto to = [](const std::vector<SendMessage>& sends, const NodeId peer)
      -> const Message& {
    const auto found = std::ranges::find_if(
        sends, [peer](const SendMessage& send) { return send.to == peer; });
    if (found == sends.end()) {
      throw std::logic_error("expected vote request was not emitted");
    }
    return found->message;
  };

  auto responses = actions_of<SendMessage>(third.step(1, to(first_requests, 3)));
  ASSERT_EQ(responses.size(), 1U);
  static_cast<void>(first.step(3, responses[0].message));
  responses = actions_of<SendMessage>(fourth.step(2, to(second_requests, 4)));
  ASSERT_EQ(responses.size(), 1U);
  static_cast<void>(second.step(4, responses[0].message));
  responses = actions_of<SendMessage>(fourth.step(1, to(first_requests, 4)));
  ASSERT_EQ(responses.size(), 1U);
  static_cast<void>(first.step(4, responses[0].message));
  responses = actions_of<SendMessage>(third.step(2, to(second_requests, 3)));
  ASSERT_EQ(responses.size(), 1U);
  static_cast<void>(second.step(3, responses[0].message));

  EXPECT_EQ(first.snapshot().role, Role::candidate);
  EXPECT_EQ(second.snapshot().role, Role::candidate);

  const auto retry = actions_of<SendMessage>(trigger_election(first));
  responses = actions_of<SendMessage>(third.step(1, to(retry, 3)));
  static_cast<void>(first.step(3, responses[0].message));
  responses = actions_of<SendMessage>(fourth.step(1, to(retry, 4)));
  static_cast<void>(first.step(4, responses[0].message));
  EXPECT_EQ(first.snapshot().role, Role::leader);
  EXPECT_EQ(first.snapshot().current_term, 2U);
}

TEST(RaftElection, HigherTermResponseCausesCandidateToStepDown) {
  RaftNode node(config());
  static_cast<void>(trigger_election(node));

  const auto actions = node.step(2, RequestVoteResponse{
                                        .term = 4,
                                        .vote_granted = false,
                                    });
  const auto state = node.snapshot();
  EXPECT_EQ(state.role, Role::follower);
  EXPECT_EQ(state.current_term, 4U);
  EXPECT_EQ(state.voted_for, std::nullopt);
  ASSERT_EQ(actions_of<PersistHardState>(actions).size(), 1U);
  EXPECT_EQ(actions_of<PersistHardState>(actions)[0].term, 4U);
}

TEST(RaftElection, StaleVoteRequestIsRejectedWithoutChangingTerm) {
  RaftNode node(config());
  static_cast<void>(trigger_election(node));

  const auto actions = node.step(2, RequestVote{
                                        .term = 0,
                                        .candidate_id = 2,
                                        .last_log_index = 0,
                                        .last_log_term = 0,
                                    });
  EXPECT_EQ(node.snapshot().current_term, 1U);
  const auto sends = actions_of<SendMessage>(actions);
  ASSERT_EQ(sends.size(), 1U);
  const auto* response = std::get_if<RequestVoteResponse>(&sends[0].message);
  ASSERT_NE(response, nullptr);
  EXPECT_FALSE(response->vote_granted);
  EXPECT_EQ(response->term, 1U);
}

TEST(RaftElection, LeaderAppendsAndReplicatesCurrentTermNoOp) {
  RaftNode node(config());
  static_cast<void>(trigger_election(node));
  const auto actions = node.step(2, RequestVoteResponse{
                                        .term = 1,
                                        .vote_granted = true,
                                    });

  const auto state = node.snapshot();
  ASSERT_EQ(state.role, Role::leader);
  ASSERT_EQ(state.log.size(), 1U);
  EXPECT_EQ(state.log[0].index, 1U);
  EXPECT_EQ(state.log[0].term, 1U);
  EXPECT_EQ(state.log[0].kind, EntryKind::no_op);
  ASSERT_EQ(actions_of<PersistLog>(actions).size(), 1U);
  EXPECT_EQ(actions_of<SendMessage>(actions).size(), 2U);

  for (const auto& send : actions_of<SendMessage>(actions)) {
    const auto* append = std::get_if<AppendEntries>(&send.message);
    ASSERT_NE(append, nullptr);
    EXPECT_EQ(append->term, 1U);
    EXPECT_EQ(append->leader_id, 1U);
    EXPECT_EQ(append->previous_log_index, 0U);
    EXPECT_EQ(append->previous_log_term, 0U);
    ASSERT_EQ(append->entries.size(), 1U);
    EXPECT_EQ(append->entries[0], state.log[0]);
    EXPECT_GT(append->rpc_id, 0U);
  }
}

TEST(RaftReplication, FollowerPersistsAppendBeforeSuccessfulResponse) {
  RaftNode node(config());
  const auto actions = node.step(
      2, append(1, 2, 0, 0, {entry(1, 1, 0xA1)}, 0, 91));

  const auto state = node.snapshot();
  ASSERT_EQ(state.log.size(), 1U);
  EXPECT_EQ(state.log[0], entry(1, 1, 0xA1));
  EXPECT_EQ(state.current_term, 1U);
  EXPECT_EQ(state.leader_id, 2U);

  const auto persists = actions_of<PersistLog>(actions);
  ASSERT_EQ(persists.size(), 1U);
  EXPECT_EQ(persists[0].from_index, 1U);
  const auto sends = actions_of<SendMessage>(actions);
  ASSERT_EQ(sends.size(), 1U);
  const auto* response = std::get_if<AppendEntriesResponse>(&sends[0].message);
  ASSERT_NE(response, nullptr);
  EXPECT_TRUE(response->success);
  EXPECT_EQ(response->match_index, 1U);
  EXPECT_EQ(response->rpc_id, 91U);

  const auto persist_position = std::ranges::find_if(
      actions, [](const Action& action) {
        return std::holds_alternative<PersistLog>(action);
      });
  const auto response_position = std::ranges::find_if(
      actions, [](const Action& action) {
        return std::holds_alternative<SendMessage>(action);
      });
  EXPECT_LT(persist_position, response_position);
}

TEST(RaftReplication, DuplicateAppendIsIdempotent) {
  RaftNode node(config());
  const auto request = append(1, 2, 0, 0, {entry(1, 1, 0xA1)}, 0, 10);
  static_cast<void>(node.step(2, request));

  auto duplicate = request;
  duplicate.rpc_id = 11;
  const auto actions = node.step(2, duplicate);
  EXPECT_TRUE(actions_of<PersistLog>(actions).empty());
  ASSERT_EQ(node.snapshot().log.size(), 1U);
  const auto sends = actions_of<SendMessage>(actions);
  ASSERT_EQ(sends.size(), 1U);
  const auto& response = std::get<AppendEntriesResponse>(sends[0].message);
  EXPECT_TRUE(response.success);
  EXPECT_EQ(response.match_index, 1U);
  EXPECT_EQ(response.rpc_id, 11U);
}

TEST(RaftReplication, MissingAndMismatchedPrefixesReturnRepairHints) {
  RaftNode node(config());
  static_cast<void>(node.step(
      2, append(2, 2, 0, 0,
                {entry(1, 1, 0x11), entry(2, 1, 0x12),
                 entry(3, 2, 0x23)},
                0, 1)));

  const auto missing = node.step(2, append(2, 2, 5, 2, {}, 0, 2));
  auto response = std::get<AppendEntriesResponse>(
      actions_of<SendMessage>(missing)[0].message);
  EXPECT_FALSE(response.success);
  EXPECT_EQ(response.reject_hint, 4U);

  const auto mismatch = node.step(2, append(2, 2, 3, 1, {}, 0, 3));
  response = std::get<AppendEntriesResponse>(
      actions_of<SendMessage>(mismatch)[0].message);
  EXPECT_FALSE(response.success);
  EXPECT_EQ(response.reject_hint, 3U);
  EXPECT_EQ(node.snapshot().log.size(), 3U);
}

TEST(RaftReplication, ConflictingSuffixIsRepairedAndCommittedInOrder) {
  RaftNode node(config());
  static_cast<void>(node.step(
      2, append(2, 2, 0, 0,
                {entry(1, 1, 0x11), entry(2, 1, 0x12),
                 entry(3, 2, 0x23)},
                0, 1)));

  const auto actions = node.step(
      3, append(3, 3, 2, 1,
                {entry(3, 3, 0x33), entry(4, 3, 0x34)}, 4, 2));
  const auto state = node.snapshot();
  ASSERT_EQ(state.log.size(), 4U);
  EXPECT_EQ(state.log[2], entry(3, 3, 0x33));
  EXPECT_EQ(state.log[3], entry(4, 3, 0x34));
  EXPECT_EQ(state.commit_index, 4U);
  EXPECT_EQ(state.last_applied, 4U);

  const auto persists = actions_of<PersistLog>(actions);
  ASSERT_EQ(persists.size(), 1U);
  EXPECT_EQ(persists[0].from_index, 3U);
  ASSERT_EQ(persists[0].entries.size(), 2U);
  const auto applied = actions_of<ApplyEntry>(actions);
  ASSERT_EQ(applied.size(), 4U);
  for (std::size_t offset = 0; offset < applied.size(); ++offset) {
    EXPECT_EQ(applied[offset].entry.index, offset + 1);
  }
}

TEST(RaftReplication, SameTermLeaderTrafficMakesCandidateStepDown) {
  RaftNode node(config());
  static_cast<void>(trigger_election(node));
  const auto actions = node.step(2, append(1, 2, 0, 0, {}, 0, 4));
  EXPECT_EQ(node.snapshot().role, Role::follower);
  EXPECT_EQ(node.snapshot().leader_id, 2U);
  ASSERT_EQ(actions_of<RoleChanged>(actions).size(), 1U);
}

TEST(RaftReplication, StaleAppendCannotChangeFollowerLogOrLeader) {
  RaftNode node(config());
  static_cast<void>(node.step(2, append(2, 2, 0, 0, {}, 0, 1)));
  const auto before = node.snapshot();

  const auto actions = node.step(
      3, append(1, 3, 0, 0, {entry(1, 1, 0xFF)}, 0, 2));
  const auto after = node.snapshot();
  EXPECT_EQ(after.current_term, before.current_term);
  EXPECT_EQ(after.leader_id, before.leader_id);
  EXPECT_EQ(after.log, before.log);
  const auto response = std::get<AppendEntriesResponse>(
      actions_of<SendMessage>(actions)[0].message);
  EXPECT_FALSE(response.success);
  EXPECT_EQ(response.term, 2U);
}

TEST(RaftReplication, HeartbeatCannotCommitUnprovenDivergentSuffix) {
  RaftNode follower(config());
  static_cast<void>(follower.step(
      2, append(2, 2, 0, 0,
                {entry(1, 1, 0x11), entry(2, 2, 0x22)}, 0, 1)));

  const auto heartbeat = follower.step(3, append(3, 3, 1, 1, {}, 2, 2));
  EXPECT_EQ(follower.snapshot().commit_index, 1U);
  EXPECT_EQ(follower.snapshot().last_applied, 1U);
  ASSERT_EQ(actions_of<ApplyEntry>(heartbeat).size(), 1U);
  EXPECT_EQ(actions_of<ApplyEntry>(heartbeat)[0].entry.index, 1U);

  const auto repair = follower.step(
      3, append(3, 3, 1, 1, {entry(2, 3, 0x32)}, 2, 3));
  EXPECT_EQ(follower.snapshot().commit_index, 2U);
  EXPECT_EQ(follower.snapshot().log[1], entry(2, 3, 0x32));
  ASSERT_EQ(actions_of<ApplyEntry>(repair).size(), 1U);
  EXPECT_EQ(actions_of<ApplyEntry>(repair)[0].entry.index, 2U);
}

TEST(RaftLeaderReplication, MajorityCommitsAndAppliesInOrder) {
  RaftNode leader(config());
  static_cast<void>(elect(leader));
  ASSERT_EQ(leader.snapshot().role, Role::leader);

  auto progress = leader.progress(2);
  ASSERT_TRUE(progress.has_value());
  auto actions = leader.step(2, AppendEntriesResponse{
                                    .term = 1,
                                    .success = true,
                                    .match_index = 1,
                                    .reject_hint = 0,
                                    .rpc_id = progress->newest_rpc_id,
                                });
  EXPECT_EQ(leader.snapshot().commit_index, 1U);
  ASSERT_EQ(actions_of<ApplyEntry>(actions).size(), 1U);
  EXPECT_EQ(actions_of<ApplyEntry>(actions)[0].entry.kind, EntryKind::no_op);

  actions = leader.propose({std::byte{0x42}});
  ASSERT_EQ(leader.snapshot().log.size(), 2U);
  progress = leader.progress(2);
  ASSERT_TRUE(progress.has_value());
  actions = leader.step(2, AppendEntriesResponse{
                               .term = 1,
                               .success = true,
                               .match_index = 2,
                               .reject_hint = 0,
                               .rpc_id = progress->newest_rpc_id,
                           });
  const auto state = leader.snapshot();
  EXPECT_EQ(state.commit_index, 2U);
  EXPECT_EQ(state.last_applied, 2U);
  ASSERT_EQ(actions_of<ApplyEntry>(actions).size(), 1U);
  EXPECT_EQ(actions_of<ApplyEntry>(actions)[0].entry.index, 2U);
  EXPECT_EQ(actions_of<ApplyEntry>(actions)[0].entry.command,
            std::vector<std::byte>{std::byte{0x42}});
}

TEST(RaftLeaderReplication, MinorityCannotCommit) {
  RaftNode leader(five_node_config());
  static_cast<void>(trigger_election(leader));
  static_cast<void>(leader.step(2, RequestVoteResponse{
                                        .term = 1,
                                        .vote_granted = true,
                                    }));
  static_cast<void>(leader.step(3, RequestVoteResponse{
                                        .term = 1,
                                        .vote_granted = true,
                                    }));
  static_cast<void>(leader.propose({std::byte{0x55}}));

  const auto progress = leader.progress(2);
  ASSERT_TRUE(progress.has_value());
  const auto actions = leader.step(2, AppendEntriesResponse{
                                          .term = 1,
                                          .success = true,
                                          .match_index = 2,
                                          .reject_hint = 0,
                                          .rpc_id = progress->newest_rpc_id,
                                      });
  EXPECT_EQ(leader.snapshot().commit_index, 0U);
  EXPECT_TRUE(actions_of<CommitAdvanced>(actions).empty());
  EXPECT_TRUE(actions_of<ApplyEntry>(actions).empty());
}

TEST(RaftLeaderReplication, DelayedAndReorderedResponsesCannotRegressProgress) {
  RaftNode leader(config());
  const auto elected = elect(leader);
  const auto initial_sends = actions_of<SendMessage>(elected);
  const auto first = std::ranges::find_if(
      initial_sends,
      [](const SendMessage& send) { return send.to == 2; });
  ASSERT_NE(first, initial_sends.end());
  const auto first_rpc = std::get<AppendEntries>(first->message).rpc_id;

  static_cast<void>(leader.propose({std::byte{0x61}}));
  auto progress = leader.progress(2);
  ASSERT_TRUE(progress.has_value());
  const auto newest_rpc = progress->newest_rpc_id;
  ASSERT_GT(newest_rpc, first_rpc);

  static_cast<void>(leader.step(2, AppendEntriesResponse{
                                        .term = 1,
                                        .success = true,
                                        .match_index = 1,
                                        .reject_hint = 0,
                                        .rpc_id = first_rpc,
                                    }));
  progress = leader.progress(2);
  ASSERT_TRUE(progress.has_value());
  EXPECT_EQ(progress->match_index, 1U);
  EXPECT_GT(progress->newest_rpc_id, newest_rpc);

  static_cast<void>(leader.step(2, AppendEntriesResponse{
                                        .term = 1,
                                        .success = true,
                                        .match_index = 2,
                                        .reject_hint = 0,
                                        .rpc_id = newest_rpc,
                                    }));
  progress = leader.progress(2);
  ASSERT_TRUE(progress.has_value());
  EXPECT_EQ(progress->match_index, 2U);
  EXPECT_EQ(progress->next_index, 3U);

  static_cast<void>(leader.step(2, AppendEntriesResponse{
                                        .term = 1,
                                        .success = false,
                                        .match_index = 0,
                                        .reject_hint = 1,
                                        .rpc_id = first_rpc,
                                    }));
  progress = leader.progress(2);
  ASSERT_TRUE(progress.has_value());
  EXPECT_EQ(progress->match_index, 2U);
  EXPECT_EQ(progress->next_index, 3U);
}

TEST(RaftLeaderReplication, ResponseMustMatchPeerAndSentLogRange) {
  RaftNode leader(config());
  const auto elected = elect(leader);
  const auto sends = actions_of<SendMessage>(elected);
  const auto to_second = std::ranges::find_if(
      sends, [](const SendMessage& send) { return send.to == 2; });
  const auto to_third = std::ranges::find_if(
      sends, [](const SendMessage& send) { return send.to == 3; });
  ASSERT_NE(to_second, sends.end());
  ASSERT_NE(to_third, sends.end());
  const auto second_rpc =
      std::get<AppendEntries>(to_second->message).rpc_id;
  const auto third_rpc = std::get<AppendEntries>(to_third->message).rpc_id;

  static_cast<void>(leader.step(2, AppendEntriesResponse{
                                        .term = 1,
                                        .success = true,
                                        .match_index = 1,
                                        .reject_hint = 0,
                                        .rpc_id = third_rpc,
                                    }));
  auto progress = leader.progress(2);
  ASSERT_TRUE(progress.has_value());
  EXPECT_EQ(progress->match_index, 0U);

  static_cast<void>(leader.propose({std::byte{0x71}}));
  static_cast<void>(leader.step(2, AppendEntriesResponse{
                                        .term = 1,
                                        .success = true,
                                        .match_index = 99,
                                        .reject_hint = 0,
                                        .rpc_id = second_rpc,
                                    }));
  progress = leader.progress(2);
  ASSERT_TRUE(progress.has_value());
  EXPECT_EQ(progress->match_index, 1U);
  EXPECT_EQ(leader.snapshot().commit_index, 1U);
}

TEST(RaftLeaderReplication, MissingSuffixIsSentInBoundedBatches) {
  auto bounded = config();
  bounded.max_append_entries = 2;
  bounded.max_append_bytes = 50;
  RaftNode leader(bounded);
  static_cast<void>(elect(leader));
  static_cast<void>(leader.propose({std::byte{0x61}}));
  static_cast<void>(leader.propose({std::byte{0x62}}));
  static_cast<void>(leader.propose({std::byte{0x63}}));

  auto progress = leader.progress(2);
  ASSERT_TRUE(progress.has_value());
  const auto actions = leader.step(2, AppendEntriesResponse{
                                          .term = 1,
                                          .success = true,
                                          .match_index = 2,
                                          .reject_hint = 0,
                                          .rpc_id = progress->newest_rpc_id,
                                      });
  const auto sends = actions_of<SendMessage>(actions);
  const auto next = std::ranges::find_if(
      sends, [](const SendMessage& send) { return send.to == 2; });
  ASSERT_NE(next, sends.end());
  const auto& append = std::get<AppendEntries>(next->message);
  ASSERT_EQ(append.entries.size(), 2U);
  EXPECT_EQ(append.entries.front().index, 3U);
  EXPECT_EQ(append.entries.back().index, 4U);
}

TEST(RaftLeaderReplication, RejectionBacktracksAndResendsTheMissingSuffix) {
  RaftNode leader(config());
  static_cast<void>(leader.step(
      2, append(1, 2, 0, 0,
                {entry(1, 1, 0x11), entry(2, 1, 0x12)}, 0, 1)));
  static_cast<void>(trigger_election(leader));
  const auto elected = leader.step(2, RequestVoteResponse{
                                          .term = 2,
                                          .vote_granted = true,
                                      });
  ASSERT_EQ(leader.snapshot().role, Role::leader);
  const auto first_progress = leader.progress(3);
  ASSERT_TRUE(first_progress.has_value());

  const auto actions = leader.step(3, AppendEntriesResponse{
                                          .term = 2,
                                          .success = false,
                                          .match_index = 0,
                                          .reject_hint = 1,
                                          .rpc_id = first_progress->newest_rpc_id,
                                      });
  const auto sends = actions_of<SendMessage>(actions);
  ASSERT_EQ(sends.size(), 1U);
  const auto& retry = std::get<AppendEntries>(sends[0].message);
  EXPECT_EQ(retry.previous_log_index, 0U);
  ASSERT_EQ(retry.entries.size(), 3U);
  EXPECT_EQ(retry.entries[0].index, 1U);
  EXPECT_EQ(retry.entries[2].term, 2U);
}

TEST(RaftLeaderReplication, PriorTermEntryCommitsOnlyThroughCurrentTermEntry) {
  RaftNode leader(config());
  static_cast<void>(leader.step(
      2, append(1, 2, 0, 0, {entry(1, 1, 0x11)}, 0, 1)));
  static_cast<void>(trigger_election(leader));
  static_cast<void>(leader.step(2, RequestVoteResponse{
                                        .term = 2,
                                        .vote_granted = true,
                                    }));
  auto progress = leader.progress(2);
  ASSERT_TRUE(progress.has_value());

  static_cast<void>(leader.step(2, AppendEntriesResponse{
                                        .term = 2,
                                        .success = true,
                                        .match_index = 1,
                                        .reject_hint = 0,
                                        .rpc_id = progress->newest_rpc_id,
                                    }));
  EXPECT_EQ(leader.snapshot().commit_index, 0U);

  static_cast<void>(leader.advance_time(leader.snapshot().heartbeat_deadline));
  progress = leader.progress(2);
  ASSERT_TRUE(progress.has_value());
  const auto actions = leader.step(2, AppendEntriesResponse{
                                          .term = 2,
                                          .success = true,
                                          .match_index = 2,
                                          .reject_hint = 0,
                                          .rpc_id = progress->newest_rpc_id,
                                      });
  EXPECT_EQ(leader.snapshot().commit_index, 2U);
  ASSERT_EQ(actions_of<ApplyEntry>(actions).size(), 2U);
  EXPECT_EQ(actions_of<ApplyEntry>(actions)[0].entry.index, 1U);
  EXPECT_EQ(actions_of<ApplyEntry>(actions)[1].entry.index, 2U);
}

TEST(RaftLeaderReplication, OldLeaderRejoinsAndAcceptsNewLeaderSuffix) {
  RaftNode old_leader(config());
  static_cast<void>(elect(old_leader));
  static_cast<void>(old_leader.propose({std::byte{0xAA}}));
  ASSERT_EQ(old_leader.snapshot().role, Role::leader);

  const auto actions = old_leader.step(
      2, append(2, 2, 1, 1, {entry(2, 2, 0xBB)}, 1, 77));
  const auto state = old_leader.snapshot();
  EXPECT_EQ(state.role, Role::follower);
  EXPECT_EQ(state.current_term, 2U);
  EXPECT_EQ(state.leader_id, 2U);
  ASSERT_EQ(state.log.size(), 2U);
  EXPECT_EQ(state.log[1], entry(2, 2, 0xBB));
  ASSERT_EQ(actions_of<PersistLog>(actions).size(), 1U);
  const auto response = std::get<AppendEntriesResponse>(
      actions_of<SendMessage>(actions)[0].message);
  EXPECT_TRUE(response.success);
}

TEST(RaftElection, CandidateWithStaleLogCannotReceiveVote) {
  RaftNode voter(config());
  static_cast<void>(voter.step(
      2, append(2, 2, 0, 0, {entry(1, 2, 0x22)}, 0, 1)));

  auto actions = voter.step(3, RequestVote{
                                   .term = 3,
                                   .candidate_id = 3,
                                   .last_log_index = 9,
                                   .last_log_term = 1,
                               });
  auto response = std::get<RequestVoteResponse>(
      actions_of<SendMessage>(actions)[0].message);
  EXPECT_FALSE(response.vote_granted);
  EXPECT_EQ(voter.snapshot().voted_for, std::nullopt);

  actions = voter.step(3, RequestVote{
                              .term = 3,
                              .candidate_id = 3,
                              .last_log_index = 1,
                              .last_log_term = 2,
                          });
  response = std::get<RequestVoteResponse>(
      actions_of<SendMessage>(actions)[0].message);
  EXPECT_TRUE(response.vote_granted);
  EXPECT_EQ(voter.snapshot().voted_for, 3U);
  const auto persist = std::ranges::find_if(
      actions, [](const Action& action) {
        return std::holds_alternative<PersistHardState>(action);
      });
  const auto send = std::ranges::find_if(actions, [](const Action& action) {
    return std::holds_alternative<SendMessage>(action);
  });
  EXPECT_LT(persist, send);
}

TEST(RaftElection, LeaderCrashAllowsHigherTermLeaderElection) {
  RaftNode first(config(1, 11));
  RaftNode second(config(2, 12));
  RaftNode third(config(3, 13));

  const auto campaign = trigger_election(first);
  const auto requests = actions_of<SendMessage>(campaign);
  const auto request_to_second = std::ranges::find_if(
      requests, [](const SendMessage& send) { return send.to == 2; });
  ASSERT_NE(request_to_second, requests.end());
  auto responses = actions_of<SendMessage>(
      second.step(1, request_to_second->message));
  ASSERT_EQ(responses.size(), 1U);
  static_cast<void>(first.step(2, responses[0].message));
  ASSERT_EQ(first.snapshot().role, Role::leader);

  const auto second_campaign = trigger_election(second);
  const auto second_requests = actions_of<SendMessage>(second_campaign);
  const auto request_to_third = std::ranges::find_if(
      second_requests, [](const SendMessage& send) { return send.to == 3; });
  ASSERT_NE(request_to_third, second_requests.end());
  responses = actions_of<SendMessage>(third.step(2, request_to_third->message));
  ASSERT_EQ(responses.size(), 1U);
  static_cast<void>(second.step(3, responses[0].message));

  EXPECT_EQ(first.snapshot().role, Role::leader);
  EXPECT_EQ(first.snapshot().current_term, 1U);
  EXPECT_EQ(second.snapshot().role, Role::leader);
  EXPECT_EQ(second.snapshot().current_term, 2U);
}

TEST(RaftLeaderReplication, HeartbeatUsesLogicalDeadlineAndCarriesCommitIndex) {
  RaftNode leader(config());
  static_cast<void>(elect(leader));
  auto progress = leader.progress(2);
  ASSERT_TRUE(progress.has_value());
  static_cast<void>(leader.step(2, AppendEntriesResponse{
                                        .term = 1,
                                        .success = true,
                                        .match_index = 1,
                                        .reject_hint = 0,
                                        .rpc_id = progress->newest_rpc_id,
                                    }));
  progress = leader.progress(3);
  ASSERT_TRUE(progress.has_value());
  static_cast<void>(leader.step(3, AppendEntriesResponse{
                                        .term = 1,
                                        .success = true,
                                        .match_index = 1,
                                        .reject_hint = 0,
                                        .rpc_id = progress->newest_rpc_id,
                                    }));
  const auto deadline = leader.snapshot().heartbeat_deadline;
  EXPECT_TRUE(leader.advance_time(deadline - 1).empty());

  const auto actions = leader.advance_time(deadline);
  const auto sends = actions_of<SendMessage>(actions);
  ASSERT_EQ(sends.size(), 2U);
  for (const auto& send : sends) {
    const auto& heartbeat = std::get<AppendEntries>(send.message);
    EXPECT_TRUE(heartbeat.entries.empty());
    EXPECT_EQ(heartbeat.leader_commit, 1U);
  }
  EXPECT_EQ(leader.snapshot().heartbeat_deadline, deadline + 50);
}

TEST(RaftProposal, FollowerRejectsWithKnownLeaderHint) {
  RaftNode follower(config());
  static_cast<void>(
      follower.step(2, append(1, 2, 0, 0, {}, 0, 1)));

  const auto actions = follower.propose({std::byte{0x19}});
  ASSERT_EQ(actions_of<ProposalRejected>(actions).size(), 1U);
  EXPECT_EQ(actions_of<ProposalRejected>(actions)[0].leader_id, 2U);
  EXPECT_TRUE(follower.snapshot().log.empty());
}

TEST(RaftReadBarrier, FollowerRejectsWithKnownLeaderHint) {
  RaftNode follower(config());
  static_cast<void>(
      follower.step(2, append(1, 2, 0, 0, {}, 0, 1)));

  const auto actions = follower.read_barrier();
  ASSERT_EQ(actions_of<ProposalRejected>(actions).size(), 1U);
  EXPECT_EQ(actions_of<ProposalRejected>(actions)[0].leader_id, 2U);
  EXPECT_TRUE(follower.snapshot().log.empty());
}

TEST(RaftReadBarrier, PersistsCurrentTermNoOpAndRequiresMajorityToApply) {
  RaftNode leader(config());
  static_cast<void>(elect(leader));

  const auto barrier = leader.read_barrier();
  const auto persisted = actions_of<PersistLog>(barrier);
  ASSERT_EQ(persisted.size(), 1U);
  ASSERT_EQ(persisted[0].entries.size(), 1U);
  EXPECT_EQ(persisted[0].entries[0].index, 2U);
  EXPECT_EQ(persisted[0].entries[0].term, 1U);
  EXPECT_EQ(persisted[0].entries[0].kind, EntryKind::no_op);
  EXPECT_TRUE(persisted[0].entries[0].command.empty());
  EXPECT_TRUE(actions_of<ApplyEntry>(barrier).empty());

  const auto sends = actions_of<SendMessage>(barrier);
  const auto peer_two = std::ranges::find_if(
      sends, [](const SendMessage& send) { return send.to == 2U; });
  ASSERT_NE(peer_two, sends.end());
  const auto& append_request = std::get<AppendEntries>(peer_two->message);
  const auto committed = leader.step(
      2, AppendEntriesResponse{.term = 1,
                               .success = true,
                               .match_index = 2,
                               .reject_hint = 0,
                               .rpc_id = append_request.rpc_id});
  const auto applied = actions_of<ApplyEntry>(committed);
  ASSERT_EQ(applied.size(), 2U);
  EXPECT_EQ(applied.back().entry, persisted[0].entries[0]);
  EXPECT_EQ(leader.snapshot().commit_index, 2U);
  EXPECT_EQ(leader.snapshot().last_applied, 2U);
}

TEST(RaftReadBarrier, HigherTermLeadershipLossRejectsDelayedBarrierAck) {
  RaftNode leader(config());
  static_cast<void>(elect(leader));
  const auto barrier = leader.read_barrier();
  const auto sends = actions_of<SendMessage>(barrier);
  const auto peer_two = std::ranges::find_if(
      sends, [](const SendMessage& send) { return send.to == 2U; });
  ASSERT_NE(peer_two, sends.end());
  const auto request = std::get<AppendEntries>(peer_two->message);

  static_cast<void>(leader.step(3, RequestVote{.term = 2,
                                               .candidate_id = 3,
                                               .last_log_index = 2,
                                               .last_log_term = 1}));
  EXPECT_EQ(leader.snapshot().role, Role::follower);
  const auto delayed = leader.step(
      2, AppendEntriesResponse{.term = 1,
                               .success = true,
                               .match_index = 2,
                               .reject_hint = 0,
                               .rpc_id = request.rpc_id});
  EXPECT_TRUE(actions_of<ApplyEntry>(delayed).empty());
  EXPECT_EQ(leader.snapshot().commit_index, 0U);
}

TEST(RaftMessages, UnknownPeerCannotChangeStateOrReceiveResponse) {
  RaftNode node(config());
  const auto before = node.snapshot();
  const auto actions = node.step(99, RequestVote{
                                        .term = 100,
                                        .candidate_id = 99,
                                        .last_log_index = 0,
                                        .last_log_term = 0,
                                    });
  EXPECT_TRUE(actions.empty());
  EXPECT_EQ(node.snapshot().current_term, before.current_term);
  EXPECT_EQ(node.snapshot().role, before.role);
}

TEST(RaftMessages, MalformedDecreasingTermLogIsRejectedAtomically) {
  RaftNode node(config());
  const auto actions = node.step(
      2, append(3, 2, 0, 0,
                {entry(1, 2, 0x21), entry(2, 1, 0x12)}, 0, 8));

  EXPECT_TRUE(node.snapshot().log.empty());
  EXPECT_TRUE(actions_of<PersistLog>(actions).empty());
  const auto sends = actions_of<SendMessage>(actions);
  ASSERT_EQ(sends.size(), 1U);
  EXPECT_FALSE(std::get<AppendEntriesResponse>(sends[0].message).success);
}

TEST(RaftMessages, TermZeroCannotGrantVoteOrEstablishLeader) {
  RaftNode node(config());
  auto actions = node.step(2, RequestVote{
                                  .term = 0,
                                  .candidate_id = 2,
                                  .last_log_index = 0,
                                  .last_log_term = 0,
                              });
  EXPECT_FALSE(std::get<RequestVoteResponse>(
                   actions_of<SendMessage>(actions)[0].message)
                   .vote_granted);
  EXPECT_EQ(node.snapshot().voted_for, std::nullopt);

  actions = node.step(2, append(0, 2, 0, 0, {}, 0, 1));
  EXPECT_FALSE(std::get<AppendEntriesResponse>(
                   actions_of<SendMessage>(actions)[0].message)
                   .success);
  EXPECT_EQ(node.snapshot().leader_id, std::nullopt);
}

TEST(RaftProposal, LeaderPersistsCommandBeforeReplication) {
  RaftNode leader(config());
  static_cast<void>(elect(leader));
  const auto actions = leader.propose({std::byte{0xCA}, std::byte{0xFE}});

  ASSERT_EQ(actions_of<PersistLog>(actions).size(), 1U);
  ASSERT_EQ(actions_of<SendMessage>(actions).size(), 2U);
  const auto persist = std::ranges::find_if(
      actions, [](const Action& action) {
        return std::holds_alternative<PersistLog>(action);
      });
  const auto first_send = std::ranges::find_if(
      actions, [](const Action& action) {
        return std::holds_alternative<SendMessage>(action);
      });
  EXPECT_LT(persist, first_send);
}

}  // namespace
}  // namespace forgekv::raft
