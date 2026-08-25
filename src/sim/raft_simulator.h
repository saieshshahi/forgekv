#pragma once

#include "raft/raft_node.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace forgekv::sim {

struct SimulatorConfig final {
  std::vector<raft::NodeId> voters;
  raft::LogicalTime election_timeout_min{};
  raft::LogicalTime election_timeout_max{};
  raft::LogicalTime heartbeat_interval{};
  std::uint64_t seed{};
  std::size_t max_pending_messages{10'000};
  std::size_t max_trace_records{2'000};
};

struct ScheduledMessage final {
  std::uint64_t id{};
  raft::NodeId from{};
  raft::NodeId to{};
  raft::Message message;
  raft::LogicalTime created_at{};
  raft::LogicalTime deliver_at{};

  bool operator==(const ScheduledMessage&) const = default;
};

struct SimulatorNodeView final {
  bool active{};
  std::optional<raft::RaftSnapshot> state;
  raft::RaftPersistentState persistent;
  std::vector<raft::LogEntry> applied;
  raft::LogIndex highest_commit_seen{};
};

class InvariantViolation final : public std::runtime_error {
 public:
  explicit InvariantViolation(std::string report);
};

class RaftSimulator final {
 public:
  explicit RaftSimulator(SimulatorConfig config);
  ~RaftSimulator();

  RaftSimulator(RaftSimulator&&) noexcept;
  RaftSimulator& operator=(RaftSimulator&&) noexcept;
  RaftSimulator(const RaftSimulator&) = delete;
  RaftSimulator& operator=(const RaftSimulator&) = delete;

  [[nodiscard]] raft::LogicalTime now() const;
  [[nodiscard]] std::uint64_t seed() const;
  [[nodiscard]] std::uint64_t operation_count() const;
  [[nodiscard]] SimulatorNodeView node(raft::NodeId node_id) const;
  [[nodiscard]] std::vector<ScheduledMessage> pending_messages() const;

  void advance_time_to(raft::LogicalTime time);
  bool deliver(std::uint64_t message_id);
  std::size_t deliver_all(std::size_t limit = 100'000);
  void delay(std::uint64_t message_id, raft::LogicalTime delay);
  void drop(std::uint64_t message_id);

  void block_link(raft::NodeId first, raft::NodeId second);
  void heal_link(raft::NodeId first, raft::NodeId second);
  void isolate(raft::NodeId node_id);
  void heal_all();

  void crash(raft::NodeId node_id);
  void restart(raft::NodeId node_id);
  void propose(raft::NodeId node_id, std::vector<std::byte> command);

  void run_random(std::size_t steps);
  void check_invariants() const;

  [[nodiscard]] std::string trace() const;
  [[nodiscard]] std::string dump() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace forgekv::sim
