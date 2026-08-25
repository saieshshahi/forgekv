#pragma once

#include "raft/types.h"

#include <memory>
#include <optional>
#include <vector>

namespace forgekv::raft {

class RaftNode final {
 public:
  explicit RaftNode(RaftConfig config);
  ~RaftNode();

  RaftNode(RaftNode&&) noexcept;
  RaftNode& operator=(RaftNode&&) noexcept;
  RaftNode(const RaftNode&) = delete;
  RaftNode& operator=(const RaftNode&) = delete;

  Actions advance_time(LogicalTime now);
  Actions step(NodeId from, const Message& message);
  Actions propose(std::vector<std::byte> command);

  [[nodiscard]] RaftSnapshot snapshot() const;
  [[nodiscard]] std::optional<PeerProgress> progress(NodeId peer) const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace forgekv::raft
