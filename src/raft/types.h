#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <variant>
#include <vector>

namespace forgekv::raft {

using NodeId = std::uint64_t;
using Term = std::uint64_t;
using LogIndex = std::uint64_t;
using LogicalTime = std::uint64_t;
using RpcId = std::uint64_t;

enum class Role : std::uint8_t {
  follower,
  candidate,
  leader,
};

enum class EntryKind : std::uint8_t {
  command,
  no_op,
};

struct LogEntry final {
  LogIndex index{};
  Term term{};
  EntryKind kind{EntryKind::command};
  std::vector<std::byte> command;

  bool operator==(const LogEntry&) const = default;
};

struct StateMachineSnapshot final {
  LogIndex last_included_index{};
  Term last_included_term{};
  std::vector<std::byte> state_machine;

  bool operator==(const StateMachineSnapshot&) const = default;
};

struct RequestVote final {
  Term term{};
  NodeId candidate_id{};
  LogIndex last_log_index{};
  Term last_log_term{};

  bool operator==(const RequestVote&) const = default;
};

struct RequestVoteResponse final {
  Term term{};
  bool vote_granted{};

  bool operator==(const RequestVoteResponse&) const = default;
};

struct AppendEntries final {
  Term term{};
  NodeId leader_id{};
  LogIndex previous_log_index{};
  Term previous_log_term{};
  std::vector<LogEntry> entries;
  LogIndex leader_commit{};
  RpcId rpc_id{};

  bool operator==(const AppendEntries&) const = default;
};

struct AppendEntriesResponse final {
  Term term{};
  bool success{};
  LogIndex match_index{};
  LogIndex reject_hint{};
  RpcId rpc_id{};

  bool operator==(const AppendEntriesResponse&) const = default;
};

struct InstallSnapshot final {
  Term term{};
  NodeId leader_id{};
  LogIndex last_included_index{};
  Term last_included_term{};
  std::uint64_t total_size{};
  std::uint64_t offset{};
  std::vector<std::byte> data;
  bool done{};
  RpcId rpc_id{};

  bool operator==(const InstallSnapshot&) const = default;
};

struct InstallSnapshotResponse final {
  Term term{};
  bool success{};
  LogIndex last_included_index{};
  std::uint64_t next_offset{};
  RpcId rpc_id{};

  bool operator==(const InstallSnapshotResponse&) const = default;
};

using Message = std::variant<RequestVote, RequestVoteResponse, AppendEntries,
                             AppendEntriesResponse, InstallSnapshot,
                             InstallSnapshotResponse>;

struct SendMessage final {
  NodeId to{};
  Message message;

  bool operator==(const SendMessage&) const = default;
};

struct PersistHardState final {
  Term term{};
  std::optional<NodeId> voted_for;

  bool operator==(const PersistHardState&) const = default;
};

struct PersistLog final {
  LogIndex from_index{};
  std::vector<LogEntry> entries;

  bool operator==(const PersistLog&) const = default;
};

struct PersistSnapshot final {
  StateMachineSnapshot snapshot;

  bool operator==(const PersistSnapshot&) const = default;
};

struct ApplySnapshot final {
  StateMachineSnapshot snapshot;

  bool operator==(const ApplySnapshot&) const = default;
};

struct RoleChanged final {
  Role from{Role::follower};
  Role to{Role::follower};
  Term term{};
  std::optional<NodeId> leader_id;

  bool operator==(const RoleChanged&) const = default;
};

struct CommitAdvanced final {
  LogIndex from_index{};
  LogIndex to_index{};

  bool operator==(const CommitAdvanced&) const = default;
};

struct ApplyEntry final {
  LogEntry entry;

  bool operator==(const ApplyEntry&) const = default;
};

struct ProposalRejected final {
  std::optional<NodeId> leader_id;

  bool operator==(const ProposalRejected&) const = default;
};

using Action =
    std::variant<SendMessage, PersistHardState, PersistLog, PersistSnapshot,
                 RoleChanged, CommitAdvanced, ApplyEntry, ApplySnapshot,
                 ProposalRejected>;
using Actions = std::vector<Action>;

struct PeerProgress final {
  struct RpcRange final {
    RpcId rpc_id{};
    LogIndex last_index{};

    bool operator==(const RpcRange&) const = default;
  };

  struct SnapshotRpcRange final {
    RpcId rpc_id{};
    LogIndex snapshot_index{};
    std::uint64_t offset{};
    std::uint64_t end{};

    bool operator==(const SnapshotRpcRange&) const = default;
  };

  LogIndex next_index{1};
  LogIndex match_index{};
  RpcId newest_rpc_id{};
  LogIndex newest_rpc_last_index{};
  std::vector<RpcRange> recent_rpcs;
  LogIndex snapshot_index{};
  std::uint64_t snapshot_offset{};
  std::vector<SnapshotRpcRange> recent_snapshot_rpcs;

  bool operator==(const PeerProgress&) const = default;
};

struct RaftConfig final {
  NodeId self_id{};
  std::uint64_t cluster_id{1};
  std::vector<NodeId> voters;
  LogicalTime election_timeout_min{};
  LogicalTime election_timeout_max{};
  LogicalTime heartbeat_interval{};
  std::uint64_t random_seed{};
  std::size_t max_append_entries{4096};
  std::size_t max_append_bytes{1'049'768};
};

struct RaftPersistentState final {
  Term current_term{};
  std::optional<NodeId> voted_for;
  std::optional<StateMachineSnapshot> snapshot;
  std::vector<LogEntry> log;
};

struct RaftSnapshot final {
  NodeId self_id{};
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
};

}  // namespace forgekv::raft
