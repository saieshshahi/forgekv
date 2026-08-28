#pragma once

#include "raft/raft_storage.h"

#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>

namespace forgekv::raft {

inline constexpr std::size_t kMaxSnapshotPayloadSize = 512U * 1024U * 1024U;

enum class SnapshotWritePoint {
  after_write,
  after_file_sync,
  after_rename,
  after_directory_sync,
};

using SnapshotWriteHook = std::function<void(SnapshotWritePoint)>;

class SnapshotStore final {
 public:
  [[nodiscard]] static std::optional<StateMachineSnapshot> load(
      const std::filesystem::path& directory, std::uint64_t cluster_id,
      NodeId node_id, std::uint64_t membership_fingerprint);

  static void write_atomic(const std::filesystem::path& directory,
                           std::uint64_t cluster_id, NodeId node_id,
                           std::uint64_t membership_fingerprint,
                           const StateMachineSnapshot& snapshot,
                           SnapshotWriteHook hook = {});

  [[nodiscard]] static std::filesystem::path published_path(
      const std::filesystem::path& directory);
  [[nodiscard]] static std::filesystem::path temporary_path(
      const std::filesystem::path& directory);
};

}  // namespace forgekv::raft
