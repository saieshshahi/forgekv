#pragma once

#include "raft/raft_node.h"
#include "raft/raft_storage.h"

#include <filesystem>
#include <chrono>
#include <functional>
#include <memory>

namespace forgekv::raft {

enum class RaftCrashPoint {
  before_persist,
  after_write,
  after_file_sync,
  after_rename,
  after_sync,
  before_response,
  after_response,
};

using RaftOutputSink = std::function<void(const Action&)>;
using RaftCrashHook = std::function<void(RaftCrashPoint)>;
using RaftSyncObserver =
    std::function<void(std::chrono::microseconds duration)>;

struct PersistedRaftOptions final {
  RaftConfig config;
  std::filesystem::path data_directory;
  LogicalTime initial_time{};
  RaftOutputSink output;
  RaftCrashHook crash_hook;
  RaftSyncObserver sync_observer{};
};

[[nodiscard]] std::uint64_t fixed_membership_fingerprint(
    std::vector<NodeId> voters);

class PersistedRaftNode final {
 public:
  [[nodiscard]] static PersistedRaftNode open(PersistedRaftOptions options);

  ~PersistedRaftNode();
  PersistedRaftNode(PersistedRaftNode&&) noexcept;
  PersistedRaftNode& operator=(PersistedRaftNode&&) noexcept;
  PersistedRaftNode(const PersistedRaftNode&) = delete;
  PersistedRaftNode& operator=(const PersistedRaftNode&) = delete;

  void advance_time(LogicalTime now);
  void step(NodeId from, const Message& message);
  void propose(std::vector<std::byte> command);
  void read_barrier();
  void compact(StateMachineSnapshot snapshot,
               bool snapshot_already_durable = false);

  [[nodiscard]] RaftSnapshot snapshot() const;
  [[nodiscard]] RaftStatus status() const noexcept;
  [[nodiscard]] std::optional<PeerProgress> progress(NodeId peer) const;
  [[nodiscard]] std::optional<LogIndex> match_index(NodeId peer) const noexcept;
  [[nodiscard]] RaftPersistentState durable_state() const;
  [[nodiscard]] bool failed() const noexcept;

 private:
  struct Impl;
  explicit PersistedRaftNode(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

}  // namespace forgekv::raft
