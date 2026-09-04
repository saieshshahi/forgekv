#include "raft/raft_node.h"

#include <algorithm>
#include <iterator>
#include <limits>
#include <random>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace forgekv::raft {
namespace {

constexpr std::size_t kMaximumTrackedRpcsPerPeer = 64U;
constexpr std::size_t kSnapshotChunkSize = 1024U * 1024U;

void validate_config_impl(const RaftConfig& config) {
  if (config.voters.size() < 3 || config.voters.size() > 7 ||
      config.voters.size() % 2 == 0) {
    throw std::invalid_argument("Raft requires 3, 5, or 7 voters");
  }
  std::unordered_set<NodeId> unique;
  for (const auto voter : config.voters) {
    if (voter == 0 || !unique.insert(voter).second) {
      throw std::invalid_argument("Raft voter IDs must be unique and nonzero");
    }
  }
  if (!unique.contains(config.self_id)) {
    throw std::invalid_argument("Raft membership must contain self ID");
  }
  if (config.election_timeout_min == 0 ||
      config.election_timeout_max < config.election_timeout_min) {
    throw std::invalid_argument("invalid Raft election timeout range");
  }
  if (config.heartbeat_interval == 0 ||
      config.heartbeat_interval >= config.election_timeout_min) {
    throw std::invalid_argument(
        "heartbeat interval must be below minimum election timeout");
  }
  if (config.max_append_entries == 0 || config.max_append_bytes < 24U) {
    throw std::invalid_argument("invalid AppendEntries batch limits");
  }
}

LogicalTime saturating_add(const LogicalTime left, const LogicalTime right) {
  if (right > std::numeric_limits<LogicalTime>::max() - left) {
    return std::numeric_limits<LogicalTime>::max();
  }
  return left + right;
}

void validate_persistent_state(const RaftConfig& config,
                               const RaftPersistentState& persistent) {
  if (persistent.current_term == 0 && persistent.voted_for.has_value()) {
    throw std::invalid_argument("term-zero persistent state cannot have a vote");
  }
  if (persistent.voted_for.has_value() &&
      std::ranges::find(config.voters, *persistent.voted_for) ==
          config.voters.end()) {
    throw std::invalid_argument("persistent Raft vote refers to a non-voter");
  }
  const auto base_index = persistent.snapshot.has_value()
                              ? persistent.snapshot->last_included_index
                              : LogIndex{0};
  Term previous_term = persistent.snapshot.has_value()
                           ? persistent.snapshot->last_included_term
                           : Term{0};
  if (persistent.snapshot.has_value() &&
      (base_index == 0 || previous_term == 0 ||
       base_index == std::numeric_limits<LogIndex>::max() ||
       previous_term > persistent.current_term)) {
    throw std::invalid_argument("persistent Raft snapshot is invalid");
  }
  LogIndex expected_index = base_index + 1;
  for (const auto& entry : persistent.log) {
    if (entry.index != expected_index || entry.term == 0 ||
        entry.term < previous_term || entry.term > persistent.current_term) {
      throw std::invalid_argument("persistent Raft log is invalid");
    }
    ++expected_index;
    previous_term = entry.term;
  }
}

}  // namespace

void validate_raft_config(const RaftConfig& config) {
  validate_config_impl(config);
}

struct RaftNode::Impl final {
  Impl(RaftConfig value, RaftPersistentState persistent,
       const LogicalTime initial_time)
      : config(std::move(value)),
        current_term(persistent.current_term),
        voted_for(persistent.voted_for),
        durable_snapshot(std::move(persistent.snapshot)),
        now(initial_time),
        random(config.random_seed ^
               (config.self_id * 0x9E3779B97F4A7C15ULL)) {
    validate_raft_config(config);
    validate_persistent_state(config, persistent);
    const auto base_index = durable_snapshot.has_value()
                                ? durable_snapshot->last_included_index
                                : LogIndex{0};
    const auto base_term = durable_snapshot.has_value()
                               ? durable_snapshot->last_included_term
                               : Term{0};
    log.push_back(LogEntry{
        .index = base_index,
        .term = base_term,
        .kind = EntryKind::no_op,
        .command = {},
    });
    log.insert(log.end(), std::make_move_iterator(persistent.log.begin()),
               std::make_move_iterator(persistent.log.end()));
    commit_index = base_index;
    last_applied = base_index;
    reset_election_deadline();
    verify_invariants();
  }

  [[nodiscard]] std::size_t quorum() const {
    return config.voters.size() / 2 + 1;
  }

  [[nodiscard]] bool is_voter(const NodeId node_id) const {
    return std::ranges::find(config.voters, node_id) != config.voters.end();
  }

  [[nodiscard]] LogIndex last_log_index() const {
    return log.back().index;
  }

  [[nodiscard]] Term last_log_term() const {
    return log.back().term;
  }

  [[nodiscard]] LogIndex base_index() const { return log.front().index; }

  [[nodiscard]] const LogEntry& entry_at(const LogIndex index) const {
    if (index < base_index() || index > last_log_index()) {
      throw std::out_of_range("Raft log index is outside retained range");
    }
    return log.at(static_cast<std::size_t>(index - base_index()));
  }

  void verify_invariants() const {
    const auto invalid = [](const char* message) {
      throw std::logic_error(message);
    };
    if (log.empty() || log.front().kind != EntryKind::no_op ||
        !log.front().command.empty() ||
        ((log.front().index == 0) != (log.front().term == 0))) {
      invalid("Raft log sentinel is invalid");
    }
    if (durable_snapshot.has_value() &&
        (durable_snapshot->last_included_index != log.front().index ||
         durable_snapshot->last_included_term != log.front().term)) {
      invalid("Raft snapshot boundary disagrees with log sentinel");
    }
    for (std::size_t position = 1; position < log.size(); ++position) {
      const auto& previous = log[position - 1];
      const auto& current = log[position];
      if (current.index != log.front().index + static_cast<LogIndex>(position) ||
          current.term < previous.term ||
          current.term > current_term) {
        invalid("Raft log is not gap-free with nondecreasing terms");
      }
    }
    if (commit_index < base_index() || commit_index > last_log_index() ||
        last_applied < base_index() || last_applied > commit_index) {
      invalid("Raft commit/apply indexes are out of bounds");
    }
    if (voted_for.has_value() && !is_voter(*voted_for)) {
      invalid("Raft vote refers to a non-voter");
    }
    if (leader_id.has_value() && !is_voter(*leader_id)) {
      invalid("Raft leader refers to a non-voter");
    }
    if (role == Role::candidate && voted_for != config.self_id) {
      invalid("Raft candidate has not voted for itself");
    }
    if (role != Role::leader) {
      if (!peer_progress.empty()) {
        invalid("non-leader retains leader-only peer progress");
      }
      return;
    }
    if (leader_id != config.self_id ||
        peer_progress.size() + 1 != config.voters.size()) {
      invalid("Raft leader identity or peer progress is incomplete");
    }
    for (const auto& [peer, progress] : peer_progress) {
      if (peer == config.self_id || !is_voter(peer) ||
          progress.match_index > last_log_index() ||
          progress.next_index < progress.match_index + 1 ||
          progress.next_index > last_log_index() + 1 ||
          progress.newest_rpc_last_index > last_log_index() ||
          progress.recent_rpcs.size() > kMaximumTrackedRpcsPerPeer) {
        invalid("Raft leader peer progress is out of bounds");
      }
      if (progress.snapshot_index != 0 &&
          (!durable_snapshot.has_value() ||
           progress.snapshot_index !=
               durable_snapshot->last_included_index ||
           progress.snapshot_offset >
               durable_snapshot->state_machine.size())) {
        invalid("Raft snapshot replication progress is out of bounds");
      }
      if (progress.recent_snapshot_rpcs.size() >
          kMaximumTrackedRpcsPerPeer) {
        invalid("too many tracked snapshot RPCs");
      }
      for (const auto& rpc : progress.recent_snapshot_rpcs) {
        if (rpc.rpc_id == 0 || rpc.rpc_id > progress.newest_rpc_id ||
            rpc.snapshot_index != progress.snapshot_index ||
            rpc.offset > rpc.end || !durable_snapshot.has_value() ||
            rpc.end > durable_snapshot->state_machine.size()) {
          invalid("Raft snapshot RPC history is out of bounds");
        }
      }
      for (const auto& rpc : progress.recent_rpcs) {
        if (rpc.rpc_id == 0 || rpc.rpc_id > progress.newest_rpc_id ||
            rpc.last_index > last_log_index()) {
          invalid("Raft leader RPC history is out of bounds");
        }
      }
    }
  }

  void reset_election_deadline() {
    std::uniform_int_distribution<LogicalTime> distribution(
        config.election_timeout_min, config.election_timeout_max);
    election_deadline = saturating_add(now, distribution(random));
  }

  void become_follower(const Term term, const std::optional<NodeId> leader) {
    const auto previous_role = role;
    const auto term_increased = term > current_term;
    if (term_increased) {
      current_term = term;
      voted_for.reset();
    }
    role = Role::follower;
    leader_id = leader;
    votes_received.clear();
    peer_progress.clear();
    reset_election_deadline();

    if (previous_role != Role::follower) {
      actions.push_back(RoleChanged{
          .from = previous_role,
          .to = Role::follower,
          .term = current_term,
          .leader_id = leader_id,
      });
    }
    if (term_increased) {
      actions.push_back(PersistHardState{
          .term = current_term,
          .voted_for = voted_for,
      });
    }
  }

  void send_append_entries(const NodeId peer) {
    auto& progress = peer_progress.at(peer);
    const auto maximum_next = last_log_index() + 1;
    progress.next_index = std::min(progress.next_index, maximum_next);
    if (progress.next_index <= base_index() && durable_snapshot.has_value()) {
      const auto& snapshot = *durable_snapshot;
      if (progress.snapshot_index != snapshot.last_included_index) {
        progress.snapshot_index = snapshot.last_included_index;
        progress.snapshot_offset = 0;
        progress.recent_snapshot_rpcs.clear();
      }
      progress.snapshot_offset = std::min<std::uint64_t>(
          progress.snapshot_offset, snapshot.state_machine.size());
      const auto remaining = snapshot.state_machine.size() -
                             static_cast<std::size_t>(progress.snapshot_offset);
      const auto chunk_size = std::min(remaining, kSnapshotChunkSize);
      const auto rpc_id = next_rpc_id++;
      progress.newest_rpc_id = rpc_id;
      progress.recent_snapshot_rpcs.push_back(PeerProgress::SnapshotRpcRange{
          .rpc_id = rpc_id,
          .snapshot_index = snapshot.last_included_index,
          .offset = progress.snapshot_offset,
          .end = progress.snapshot_offset + chunk_size,
      });
      if (progress.recent_snapshot_rpcs.size() >
          kMaximumTrackedRpcsPerPeer) {
        progress.recent_snapshot_rpcs.erase(
            progress.recent_snapshot_rpcs.begin());
      }
      const auto begin = snapshot.state_machine.begin() +
                         static_cast<std::ptrdiff_t>(progress.snapshot_offset);
      actions.push_back(SendMessage{
          .to = peer,
          .message = InstallSnapshot{
              .term = current_term,
              .leader_id = config.self_id,
              .last_included_index = snapshot.last_included_index,
              .last_included_term = snapshot.last_included_term,
              .total_size = snapshot.state_machine.size(),
              .offset = progress.snapshot_offset,
              .data = {begin,
                       begin + static_cast<std::ptrdiff_t>(chunk_size)},
              .done = chunk_size == remaining,
              .rpc_id = rpc_id,
          },
      });
      return;
    }
    progress.next_index = std::max(progress.next_index, base_index() + 1);
    const auto previous_index = progress.next_index - 1;
    const auto previous_term = entry_at(previous_index).term;
    std::vector<LogEntry> entries;
    std::size_t encoded_bytes = 0;
    for (auto position = static_cast<std::size_t>(progress.next_index - base_index());
         position < log.size() &&
         entries.size() < config.max_append_entries;
         ++position) {
      const auto entry_bytes = 24U + log[position].command.size();
      if (!entries.empty() &&
          (encoded_bytes >= config.max_append_bytes ||
           entry_bytes > config.max_append_bytes - encoded_bytes)) {
        break;
      }
      entries.push_back(log[position]);
      encoded_bytes += entry_bytes;
    }
    const auto rpc_id = next_rpc_id++;
    progress.newest_rpc_id = rpc_id;
    progress.newest_rpc_last_index =
        entries.empty() ? previous_index : entries.back().index;
    progress.recent_rpcs.push_back(PeerProgress::RpcRange{
        .rpc_id = rpc_id,
        .last_index = progress.newest_rpc_last_index,
    });
    if (progress.recent_rpcs.size() > kMaximumTrackedRpcsPerPeer) {
      progress.recent_rpcs.erase(progress.recent_rpcs.begin());
    }
    actions.push_back(SendMessage{
        .to = peer,
        .message = AppendEntries{
            .term = current_term,
            .leader_id = config.self_id,
            .previous_log_index = previous_index,
            .previous_log_term = previous_term,
            .entries = std::move(entries),
            .leader_commit = commit_index,
            .rpc_id = rpc_id,
        },
    });
  }

  void broadcast_append_entries() {
    for (const auto voter : config.voters) {
      if (voter != config.self_id) {
        send_append_entries(voter);
      }
    }
  }

  void append_local_entry(const EntryKind kind, std::vector<std::byte> command) {
    LogEntry entry{
        .index = last_log_index() + 1,
        .term = current_term,
        .kind = kind,
        .command = std::move(command),
    };
    log.push_back(entry);
    actions.push_back(PersistLog{
        .from_index = entry.index,
        .entries = {std::move(entry)},
    });
  }

  void become_leader() {
    const auto previous_role = role;
    role = Role::leader;
    leader_id = config.self_id;
    votes_received.clear();
    peer_progress.clear();
    const auto next_index = last_log_index() + 1;
    for (const auto voter : config.voters) {
      if (voter != config.self_id) {
        peer_progress.emplace(voter, PeerProgress{
                                          .next_index = next_index,
                                          .match_index = 0,
                                          .newest_rpc_id = 0,
                                          .newest_rpc_last_index = 0,
                                          .recent_rpcs = {},
                                          .snapshot_index = 0,
                                          .snapshot_offset = 0,
                                          .recent_snapshot_rpcs = {},
                                      });
      }
    }
    heartbeat_deadline = saturating_add(now, config.heartbeat_interval);
    actions.push_back(RoleChanged{
        .from = previous_role,
        .to = Role::leader,
        .term = current_term,
        .leader_id = leader_id,
    });
    append_local_entry(EntryKind::no_op, {});
    broadcast_append_entries();
  }

  void start_election() {
    if (current_term == std::numeric_limits<Term>::max()) {
      throw std::overflow_error("Raft term space exhausted");
    }
    const auto previous_role = role;
    ++current_term;
    role = Role::candidate;
    leader_id.reset();
    voted_for = config.self_id;
    votes_received.clear();
    votes_received.insert(config.self_id);
    reset_election_deadline();

    if (previous_role != Role::candidate) {
      actions.push_back(RoleChanged{
          .from = previous_role,
          .to = Role::candidate,
          .term = current_term,
          .leader_id = std::nullopt,
      });
    }
    actions.push_back(PersistHardState{
        .term = current_term,
        .voted_for = voted_for,
    });

    const RequestVote request{
        .term = current_term,
        .candidate_id = config.self_id,
        .last_log_index = last_log_index(),
        .last_log_term = last_log_term(),
    };
    for (const auto voter : config.voters) {
      if (voter != config.self_id) {
        actions.push_back(SendMessage{.to = voter, .message = request});
      }
    }
  }

  void handle(const NodeId from, const RequestVote& request) {
    const auto up_to_date =
        request.last_log_term > last_log_term() ||
        (request.last_log_term == last_log_term() &&
         request.last_log_index >= last_log_index());
    const auto candidate_matches_source = request.candidate_id == from;
    const auto can_vote = !voted_for.has_value() || voted_for == from;
    const auto grant = request.term != 0 && request.term == current_term &&
                       candidate_matches_source && can_vote && up_to_date;
    if (grant && voted_for != from) {
      voted_for = from;
      actions.push_back(PersistHardState{
          .term = current_term,
          .voted_for = voted_for,
      });
    }
    if (grant) {
      reset_election_deadline();
    }
    actions.push_back(SendMessage{
        .to = from,
        .message = RequestVoteResponse{
            .term = current_term,
            .vote_granted = grant,
        },
    });
  }

  void handle(const NodeId from, const RequestVoteResponse& response) {
    if (role != Role::candidate || response.term != current_term ||
        !response.vote_granted) {
      return;
    }
    votes_received.insert(from);
    if (votes_received.size() >= quorum()) {
      become_leader();
    }
  }

  void send_append_response(const NodeId to, const AppendEntries& request,
                            const bool success, const LogIndex match_index,
                            const LogIndex reject_hint) {
    actions.push_back(SendMessage{
        .to = to,
        .message = AppendEntriesResponse{
            .term = current_term,
            .success = success,
            .match_index = match_index,
            .reject_hint = reject_hint,
            .rpc_id = request.rpc_id,
        },
    });
  }

  void advance_follower_commit(const LogIndex leader_commit) {
    const auto new_commit = std::min(leader_commit, last_log_index());
    if (new_commit <= commit_index) {
      return;
    }
    const auto previous_commit = commit_index;
    commit_index = new_commit;
    actions.push_back(CommitAdvanced{
        .from_index = previous_commit,
        .to_index = commit_index,
    });
    while (last_applied < commit_index) {
      ++last_applied;
      actions.push_back(ApplyEntry{
          .entry = entry_at(last_applied),
      });
    }
  }

  bool advance_leader_commit() {
    for (auto candidate = last_log_index(); candidate > commit_index;
         --candidate) {
      if (entry_at(candidate).term != current_term) {
        continue;
      }
      std::size_t replicated = 1;
      for (const auto& [peer, progress] : peer_progress) {
        static_cast<void>(peer);
        if (progress.match_index >= candidate) {
          ++replicated;
        }
      }
      if (replicated < quorum()) {
        continue;
      }

      const auto previous_commit = commit_index;
      commit_index = candidate;
      actions.push_back(CommitAdvanced{
          .from_index = previous_commit,
          .to_index = commit_index,
      });
      while (last_applied < commit_index) {
        ++last_applied;
        actions.push_back(ApplyEntry{
            .entry = entry_at(last_applied),
        });
      }
      return true;
    }
    return false;
  }

  void handle(const NodeId from, const AppendEntries& request) {
    if (request.term == 0 || request.term < current_term ||
        request.leader_id != from) {
      send_append_response(from, request, false, 0, last_log_index() + 1);
      return;
    }

    if (role != Role::follower) {
      become_follower(current_term, from);
    } else {
      leader_id = from;
      reset_election_deadline();
    }

    if (request.previous_log_index < base_index()) {
      send_append_response(from, request, false, 0, base_index() + 1);
      return;
    }
    if (request.previous_log_index > last_log_index()) {
      send_append_response(from, request, false, 0, last_log_index() + 1);
      return;
    }
    const auto local_previous_term = entry_at(request.previous_log_index).term;
    if (local_previous_term != request.previous_log_term) {
      auto first_conflict = request.previous_log_index;
      while (first_conflict > base_index() + 1 &&
             entry_at(first_conflict - 1).term ==
                 local_previous_term) {
        --first_conflict;
      }
      send_append_response(from, request, false, 0, first_conflict);
      return;
    }

    auto expected_index = request.previous_log_index + 1;
    auto expected_minimum_term = request.previous_log_term;
    for (const auto& incoming : request.entries) {
      if (incoming.index != expected_index || incoming.term > request.term ||
          incoming.term < expected_minimum_term || incoming.index == 0) {
        send_append_response(from, request, false, 0, expected_index);
        return;
      }
      ++expected_index;
      expected_minimum_term = incoming.term;
    }

    std::optional<std::size_t> replacement_offset;
    for (std::size_t offset = 0; offset < request.entries.size(); ++offset) {
      const auto& incoming = request.entries[offset];
      if (incoming.index > last_log_index()) {
        replacement_offset = offset;
        break;
      }
      const auto& existing = entry_at(incoming.index);
      if (existing.term != incoming.term) {
        if (incoming.index <= commit_index) {
          send_append_response(from, request, false, 0, commit_index + 1);
          return;
        }
        replacement_offset = offset;
        break;
      }
      if (existing != incoming) {
        send_append_response(from, request, false, 0, incoming.index);
        return;
      }
    }

    if (replacement_offset.has_value()) {
      const auto offset = *replacement_offset;
      const auto from_index = request.entries[offset].index;
      log.resize(static_cast<std::size_t>(from_index - base_index()));
      log.insert(log.end(), request.entries.begin() +
                                static_cast<std::ptrdiff_t>(offset),
                 request.entries.end());
      actions.push_back(PersistLog{
          .from_index = from_index,
          .entries = {request.entries.begin() +
                          static_cast<std::ptrdiff_t>(offset),
                      request.entries.end()},
      });
    }

    const auto matched_index =
        request.previous_log_index +
        static_cast<LogIndex>(request.entries.size());
    advance_follower_commit(std::min(request.leader_commit, matched_index));
    send_append_response(from, request, true, matched_index, 0);
  }

  void handle(const NodeId from, const AppendEntriesResponse& response) {
    if (role != Role::leader || response.term != current_term) {
      return;
    }
    const auto found = peer_progress.find(from);
    if (found == peer_progress.end()) {
      return;
    }
    auto& progress = found->second;
    const auto rpc = std::ranges::find_if(
        progress.recent_rpcs, [&response](const PeerProgress::RpcRange& sent) {
          return sent.rpc_id == response.rpc_id;
        });
    if (rpc == progress.recent_rpcs.end()) {
      return;
    }

    if (response.success) {
      const auto reported = std::min(response.match_index, rpc->last_index);
      progress.recent_rpcs.erase(rpc);
      progress.match_index = std::max(progress.match_index, reported);
      progress.next_index = progress.match_index + 1;
      if (advance_leader_commit()) {
        broadcast_append_entries();
      } else if (progress.next_index <= last_log_index()) {
        send_append_entries(from);
      }
      return;
    }

    if (response.rpc_id != progress.newest_rpc_id) {
      return;
    }
    progress.recent_rpcs.erase(rpc);

    const auto minimum_next = progress.match_index + 1;
    const auto one_back = progress.next_index > 1 ? progress.next_index - 1 : 1;
    auto requested_next = response.reject_hint;
    if (requested_next == 0 || requested_next >= progress.next_index) {
      requested_next = one_back;
    }
    progress.next_index = std::max(minimum_next, requested_next);
    send_append_entries(from);
  }

  void send_snapshot_response(const NodeId to, const InstallSnapshot& request,
                              const bool success,
                              const std::uint64_t next_offset) {
    actions.push_back(SendMessage{
        .to = to,
        .message = InstallSnapshotResponse{
            .term = current_term,
            .success = success,
            .last_included_index = request.last_included_index,
            .next_offset = next_offset,
            .rpc_id = request.rpc_id,
        },
    });
  }

  void handle(const NodeId from, const InstallSnapshot& request) {
    if (request.term == 0 || request.term < current_term ||
        request.leader_id != from || request.rpc_id == 0 ||
        request.last_included_index == 0 ||
        request.last_included_index ==
            std::numeric_limits<LogIndex>::max() ||
        request.last_included_term == 0 ||
        request.last_included_term > request.term ||
        request.total_size > 512ULL * 1024ULL * 1024ULL ||
        request.offset > request.total_size ||
        request.data.size() > request.total_size - request.offset ||
        request.done !=
            (request.offset + request.data.size() == request.total_size)) {
      send_snapshot_response(from, request, false, 0);
      return;
    }
    if (role != Role::follower) {
      become_follower(current_term, from);
    } else {
      leader_id = from;
      reset_election_deadline();
    }
    if (request.last_included_index <= last_applied) {
      send_snapshot_response(from, request, true, request.total_size);
      return;
    }
    if (request.offset == 0) {
      incoming_snapshot = IncomingSnapshot{
          .leader_id = from,
          .last_included_index = request.last_included_index,
          .last_included_term = request.last_included_term,
          .total_size = request.total_size,
          .bytes = {},
      };
      incoming_snapshot->bytes.reserve(
          static_cast<std::size_t>(request.total_size));
    }
    if (!incoming_snapshot.has_value() ||
        incoming_snapshot->leader_id != from ||
        incoming_snapshot->last_included_index !=
            request.last_included_index ||
        incoming_snapshot->last_included_term != request.last_included_term ||
        incoming_snapshot->total_size != request.total_size ||
        incoming_snapshot->bytes.size() != request.offset) {
      const auto expected = incoming_snapshot.has_value()
                                ? incoming_snapshot->bytes.size()
                                : std::uint64_t{0};
      send_snapshot_response(from, request, false, expected);
      return;
    }
    incoming_snapshot->bytes.insert(incoming_snapshot->bytes.end(),
                                    request.data.begin(), request.data.end());
    if (!request.done) {
      send_snapshot_response(from, request, true,
                             incoming_snapshot->bytes.size());
      return;
    }

    StateMachineSnapshot installed{
        .last_included_index = request.last_included_index,
        .last_included_term = request.last_included_term,
        .state_machine = std::move(incoming_snapshot->bytes),
    };
    incoming_snapshot.reset();
    std::vector<LogEntry> suffix;
    if (installed.last_included_index <= last_log_index() &&
        installed.last_included_index >= base_index() &&
        entry_at(installed.last_included_index).term ==
            installed.last_included_term) {
      const auto position = static_cast<std::size_t>(
          installed.last_included_index - base_index() + 1);
      suffix.assign(log.begin() + static_cast<std::ptrdiff_t>(position),
                    log.end());
    }
    log.clear();
    log.push_back(LogEntry{.index = installed.last_included_index,
                           .term = installed.last_included_term,
                           .kind = EntryKind::no_op,
                           .command = {}});
    log.insert(log.end(), std::make_move_iterator(suffix.begin()),
               std::make_move_iterator(suffix.end()));
    durable_snapshot = installed;
    commit_index = std::max(commit_index, installed.last_included_index);
    commit_index = std::min(commit_index, last_log_index());
    last_applied = installed.last_included_index;
    actions.push_back(PersistSnapshot{.snapshot = installed});
    actions.push_back(ApplySnapshot{.snapshot = installed});
    send_snapshot_response(from, request, true, request.total_size);
  }

  void handle(const NodeId from, const InstallSnapshotResponse& response) {
    if (role != Role::leader || response.term != current_term) {
      return;
    }
    const auto found = peer_progress.find(from);
    if (found == peer_progress.end() || !durable_snapshot.has_value() ||
        response.last_included_index !=
            durable_snapshot->last_included_index ||
        found->second.snapshot_index !=
            durable_snapshot->last_included_index) {
      return;
    }
    auto& progress = found->second;
    const auto rpc = std::ranges::find_if(
        progress.recent_snapshot_rpcs,
        [&response](const PeerProgress::SnapshotRpcRange& sent) {
          return sent.rpc_id == response.rpc_id &&
                 sent.snapshot_index == response.last_included_index;
        });
    if (rpc == progress.recent_snapshot_rpcs.end()) {
      return;
    }
    const auto sent = *rpc;
    if (!response.success) {
      if (response.rpc_id != progress.newest_rpc_id ||
          response.next_offset > sent.end) {
        return;
      }
      progress.recent_snapshot_rpcs.erase(rpc);
      progress.snapshot_offset = response.next_offset;
      send_append_entries(from);
      return;
    }
    if (response.next_offset != sent.end) {
      return;
    }
    if (sent.end <= progress.snapshot_offset) {
      return;
    }
    if (sent.offset != progress.snapshot_offset) {
      return;
    }
    progress.recent_snapshot_rpcs.erase(rpc);
    progress.snapshot_offset = sent.end;
    if (progress.snapshot_offset < durable_snapshot->state_machine.size()) {
      send_append_entries(from);
      return;
    }
    progress.match_index =
        std::max(progress.match_index, durable_snapshot->last_included_index);
    progress.next_index = progress.match_index + 1;
    progress.snapshot_offset = 0;
    progress.snapshot_index = 0;
    progress.recent_snapshot_rpcs.clear();
    progress.recent_rpcs.clear();
    send_append_entries(from);
  }

  [[nodiscard]] Term message_term(const Message& message) const {
    return std::visit([](const auto& value) { return value.term; }, message);
  }

  void step(const NodeId from, const Message& message) {
    if (from == config.self_id || !is_voter(from)) {
      return;
    }
    const auto incoming_term = message_term(message);
    if (incoming_term > current_term) {
      std::optional<NodeId> incoming_leader;
      if (const auto* append = std::get_if<AppendEntries>(&message);
          append != nullptr && append->leader_id == from) {
        incoming_leader = from;
      } else if (const auto* snapshot = std::get_if<InstallSnapshot>(&message);
                 snapshot != nullptr && snapshot->leader_id == from) {
        incoming_leader = from;
      }
      become_follower(incoming_term, incoming_leader);
    }
    std::visit([this, from](const auto& value) { handle(from, value); },
               message);
  }

  RaftConfig config;
  Role role{Role::follower};
  Term current_term{};
  std::optional<NodeId> voted_for;
  std::optional<NodeId> leader_id;
  std::optional<StateMachineSnapshot> durable_snapshot;
  std::vector<LogEntry> log;
  LogIndex commit_index{};
  LogIndex last_applied{};
  LogicalTime now{};
  LogicalTime election_deadline{};
  LogicalTime heartbeat_deadline{};
  std::mt19937_64 random;
  std::unordered_set<NodeId> votes_received;
  std::unordered_map<NodeId, PeerProgress> peer_progress;
  struct IncomingSnapshot final {
    NodeId leader_id{};
    LogIndex last_included_index{};
    Term last_included_term{};
    std::uint64_t total_size{};
    std::vector<std::byte> bytes;
  };
  std::optional<IncomingSnapshot> incoming_snapshot;
  RpcId next_rpc_id{1};
  Actions actions;
};

RaftNode::RaftNode(RaftConfig config)
    : RaftNode(std::move(config), RaftPersistentState{}, 0) {}

RaftNode::RaftNode(RaftConfig config, RaftPersistentState persistent_state,
                   const LogicalTime initial_time)
    : impl_(std::make_unique<Impl>(std::move(config),
                                   std::move(persistent_state), initial_time)) {}

RaftNode::~RaftNode() = default;
RaftNode::RaftNode(RaftNode&&) noexcept = default;
RaftNode& RaftNode::operator=(RaftNode&&) noexcept = default;

Actions RaftNode::advance_time(const LogicalTime now) {
  if (now < impl_->now) {
    throw std::invalid_argument("Raft logical time cannot move backward");
  }
  impl_->now = now;
  impl_->actions.clear();
  if (impl_->role != Role::leader && now >= impl_->election_deadline) {
    impl_->start_election();
  } else if (impl_->role == Role::leader &&
             now >= impl_->heartbeat_deadline) {
    impl_->broadcast_append_entries();
    impl_->heartbeat_deadline =
        saturating_add(now, impl_->config.heartbeat_interval);
  }
  impl_->verify_invariants();
  return std::move(impl_->actions);
}

Actions RaftNode::step(const NodeId from, const Message& message) {
  impl_->actions.clear();
  impl_->step(from, message);
  impl_->verify_invariants();
  return std::move(impl_->actions);
}

Actions RaftNode::propose(std::vector<std::byte> command) {
  impl_->actions.clear();
  if (impl_->role != Role::leader) {
    impl_->actions.push_back(ProposalRejected{.leader_id = impl_->leader_id});
  } else {
    impl_->append_local_entry(EntryKind::command, std::move(command));
    impl_->broadcast_append_entries();
  }
  impl_->verify_invariants();
  return std::move(impl_->actions);
}

Actions RaftNode::read_barrier() {
  impl_->actions.clear();
  if (impl_->role != Role::leader) {
    impl_->actions.push_back(ProposalRejected{.leader_id = impl_->leader_id});
  } else {
    impl_->append_local_entry(EntryKind::no_op, {});
    impl_->broadcast_append_entries();
  }
  impl_->verify_invariants();
  return std::move(impl_->actions);
}

Actions RaftNode::compact(StateMachineSnapshot snapshot) {
  impl_->actions.clear();
  if (snapshot.last_included_index ==
          std::numeric_limits<LogIndex>::max() ||
      snapshot.last_included_index <= impl_->base_index() ||
      snapshot.last_included_index > impl_->last_applied ||
      impl_->entry_at(snapshot.last_included_index).term !=
          snapshot.last_included_term) {
    throw std::invalid_argument("snapshot does not match an applied log prefix");
  }
  const auto suffix_position = static_cast<std::size_t>(
      snapshot.last_included_index - impl_->base_index() + 1);
  std::vector<LogEntry> suffix{
      impl_->log.begin() + static_cast<std::ptrdiff_t>(suffix_position),
      impl_->log.end()};
  impl_->log.clear();
  impl_->log.push_back(LogEntry{.index = snapshot.last_included_index,
                                .term = snapshot.last_included_term,
                                .kind = EntryKind::no_op,
                                .command = {}});
  impl_->log.insert(impl_->log.end(), std::make_move_iterator(suffix.begin()),
                    std::make_move_iterator(suffix.end()));
  impl_->durable_snapshot = snapshot;
  for (auto& [peer, progress] : impl_->peer_progress) {
    static_cast<void>(peer);
    progress.snapshot_index = snapshot.last_included_index;
    progress.snapshot_offset = 0;
    progress.recent_snapshot_rpcs.clear();
  }
  impl_->actions.push_back(PersistSnapshot{.snapshot = std::move(snapshot)});
  impl_->verify_invariants();
  return std::move(impl_->actions);
}

RaftSnapshot RaftNode::snapshot() const {
  return RaftSnapshot{
      .self_id = impl_->config.self_id,
      .role = impl_->role,
      .current_term = impl_->current_term,
      .voted_for = impl_->voted_for,
      .leader_id = impl_->leader_id,
      .durable_snapshot = impl_->durable_snapshot,
      .log = {impl_->log.begin() + 1, impl_->log.end()},
      .commit_index = impl_->commit_index,
      .last_applied = impl_->last_applied,
      .now = impl_->now,
      .election_deadline = impl_->election_deadline,
      .heartbeat_deadline = impl_->heartbeat_deadline,
  };
}

RaftStatus RaftNode::status() const noexcept {
  return RaftStatus{
      .self_id = impl_->config.self_id,
      .role = impl_->role,
      .current_term = impl_->current_term,
      .leader_id = impl_->leader_id,
      .commit_index = impl_->commit_index,
      .last_applied = impl_->last_applied,
      .last_log_index = impl_->last_log_index(),
      .retained_log_records =
          static_cast<std::uint64_t>(impl_->log.size() - 1U),
  };
}

std::optional<PeerProgress> RaftNode::progress(const NodeId peer) const {
  const auto found = impl_->peer_progress.find(peer);
  if (found == impl_->peer_progress.end()) {
    return std::nullopt;
  }
  return found->second;
}

std::optional<LogIndex> RaftNode::match_index(const NodeId peer) const noexcept {
  const auto found = impl_->peer_progress.find(peer);
  if (found == impl_->peer_progress.end()) {
    return std::nullopt;
  }
  return found->second.match_index;
}

}  // namespace forgekv::raft
