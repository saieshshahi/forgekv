#pragma once

#include "protocol/frame.h"
#include "raft/types.h"

#include <filesystem>
#include <functional>
#include <memory>
#include <stdexcept>

namespace forgekv::raft {

inline constexpr std::size_t kMaxRaftCommandSize =
    protocol::kMaxClientCommandSize;
inline constexpr std::size_t kMaxRaftLogEntriesPerRecord = 4096U;
inline constexpr std::size_t kMaxRaftLogRecordSize = 64U * 1024U * 1024U;

enum class RaftStorageErrorCode {
  io,
  corruption,
  invalid_update,
  closed,
};

enum class RaftStorageSyncPoint {
  after_file_sync,
  after_rename,
};

using RaftStorageSyncHook = std::function<void(RaftStorageSyncPoint)>;

class RaftStorageError final : public std::runtime_error {
 public:
  RaftStorageError(RaftStorageErrorCode code, const std::string& message);
  [[nodiscard]] RaftStorageErrorCode code() const noexcept;

 private:
  RaftStorageErrorCode code_;
};

class RaftStorage final {
 public:
  [[nodiscard]] static RaftStorage open(const std::filesystem::path& directory,
                                        std::uint64_t cluster_id,
                                        NodeId node_id,
                                        RaftStorageSyncHook sync_hook = {},
                                        std::uint64_t membership_fingerprint = 0);

  ~RaftStorage();
  RaftStorage(RaftStorage&&) noexcept;
  RaftStorage& operator=(RaftStorage&&) noexcept;
  RaftStorage(const RaftStorage&) = delete;
  RaftStorage& operator=(const RaftStorage&) = delete;

  void prepare(const PersistHardState& update);
  void prepare(const PersistLog& update);
  void sync();
  void close() noexcept;

  [[nodiscard]] const RaftPersistentState& state() const noexcept;
  [[nodiscard]] bool has_pending_update() const noexcept;

 private:
  struct Impl;
  explicit RaftStorage(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

}  // namespace forgekv::raft
