#include "raft/snapshot_store.h"

#include "protocol/checksum.h"
#include "protocol/wire.h"

#include <cerrno>
#include <cstring>
#include <limits>
#include <mutex>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace forgekv::raft {
namespace {

namespace wire = forgekv::protocol::wire;

constexpr std::uint32_t kSnapshotMagic = 0x4652534EU;
constexpr std::uint16_t kSnapshotFormatVersion = 1U;
constexpr std::size_t kSnapshotHeaderSize = 64U;
std::mutex snapshot_publish_mutex;

[[noreturn]] void throw_io(const std::string& action) {
  throw RaftStorageError(RaftStorageErrorCode::io,
                         action + ": " + std::string(std::strerror(errno)));
}

[[noreturn]] void throw_corruption(const std::string& message) {
  throw RaftStorageError(RaftStorageErrorCode::corruption, message);
}

[[noreturn]] void throw_invalid(const std::string& message) {
  throw RaftStorageError(RaftStorageErrorCode::invalid_update, message);
}

void write_u16(const std::span<std::byte> destination,
               const std::uint16_t value) {
  destination[0] = static_cast<std::byte>((value >> 8U) & 0xFFU);
  destination[1] = static_cast<std::byte>(value & 0xFFU);
}

std::uint16_t read_u16(const std::span<const std::byte> source) {
  return static_cast<std::uint16_t>(
      (std::to_integer<std::uint16_t>(source[0]) << 8U) |
      std::to_integer<std::uint16_t>(source[1]));
}

void close_file(const int descriptor) noexcept {
  if (descriptor >= 0) {
    static_cast<void>(::close(descriptor));
  }
}

void write_all(const int descriptor, const std::span<const std::byte> bytes) {
  std::size_t offset = 0;
  while (offset < bytes.size()) {
    const auto count =
        ::write(descriptor, bytes.data() + offset, bytes.size() - offset);
    if (count > 0) {
      offset += static_cast<std::size_t>(count);
    } else if (count < 0 && errno == EINTR) {
      continue;
    } else {
      if (count == 0) {
        errno = EIO;
      }
      throw_io("write Raft snapshot");
    }
  }
}

void read_all(const int descriptor, const std::span<std::byte> bytes) {
  std::size_t offset = 0;
  while (offset < bytes.size()) {
    const auto count =
        ::read(descriptor, bytes.data() + offset, bytes.size() - offset);
    if (count > 0) {
      offset += static_cast<std::size_t>(count);
    } else if (count == 0) {
      throw_corruption("truncated Raft snapshot");
    } else if (errno != EINTR) {
      throw_io("read Raft snapshot");
    }
  }
}

void sync_file(const int descriptor) {
  while (::fdatasync(descriptor) != 0) {
    if (errno != EINTR) {
      throw_io("sync Raft snapshot");
    }
  }
}

void sync_directory(const std::filesystem::path& directory) {
  const auto descriptor =
      ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (descriptor < 0) {
    throw_io("open snapshot directory for sync");
  }
  try {
    while (::fsync(descriptor) != 0) {
      if (errno != EINTR) {
        throw_io("sync snapshot directory");
      }
    }
  } catch (...) {
    close_file(descriptor);
    throw;
  }
  close_file(descriptor);
}

std::vector<std::byte> header(const std::uint64_t cluster_id,
                              const NodeId node_id,
                              const std::uint64_t membership_fingerprint,
                              const StateMachineSnapshot& snapshot) {
  std::vector<std::byte> result(kSnapshotHeaderSize);
  wire::write_u32(std::span{result}.subspan(0, 4), kSnapshotMagic);
  write_u16(std::span{result}.subspan(4, 2), kSnapshotFormatVersion);
  write_u16(std::span{result}.subspan(6, 2),
            static_cast<std::uint16_t>(kSnapshotHeaderSize));
  wire::write_u64(std::span{result}.subspan(8, 8), cluster_id);
  wire::write_u64(std::span{result}.subspan(16, 8), node_id);
  wire::write_u64(std::span{result}.subspan(24, 8), membership_fingerprint);
  wire::write_u64(std::span{result}.subspan(32, 8),
                  snapshot.last_included_index);
  wire::write_u64(std::span{result}.subspan(40, 8),
                  snapshot.last_included_term);
  wire::write_u64(std::span{result}.subspan(48, 8),
                  snapshot.state_machine.size());
  wire::write_u32(std::span{result}.subspan(56, 4),
                  protocol::crc32(snapshot.state_machine));
  wire::write_u32(std::span{result}.subspan(60, 4),
                  protocol::crc32(std::span<const std::byte>{result}.first(60)));
  return result;
}

}  // namespace

std::filesystem::path SnapshotStore::published_path(
    const std::filesystem::path& directory) {
  return directory / "raft.snapshot";
}

std::filesystem::path SnapshotStore::temporary_path(
    const std::filesystem::path& directory) {
  return directory / "raft.snapshot.tmp";
}

std::optional<StateMachineSnapshot> SnapshotStore::load(
    const std::filesystem::path& directory, const std::uint64_t cluster_id,
    const NodeId node_id, const std::uint64_t membership_fingerprint) {
  const auto path = published_path(directory);
  const auto descriptor = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
  if (descriptor < 0) {
    if (errno == ENOENT) {
      return std::nullopt;
    }
    throw_io("open Raft snapshot");
  }

  try {
    struct stat status {};
    if (::fstat(descriptor, &status) != 0) {
      throw_io("stat Raft snapshot");
    }
    if (status.st_size < static_cast<off_t>(kSnapshotHeaderSize)) {
      throw_corruption("Raft snapshot is shorter than its header");
    }
    std::vector<std::byte> fixed(kSnapshotHeaderSize);
    read_all(descriptor, fixed);
    const auto fixed_span = std::span<const std::byte>{fixed};
    if (wire::read_u32(fixed_span.subspan(0, 4)) != kSnapshotMagic ||
        read_u16(fixed_span.subspan(4, 2)) != kSnapshotFormatVersion ||
        read_u16(fixed_span.subspan(6, 2)) != kSnapshotHeaderSize ||
        wire::read_u64(fixed_span.subspan(8, 8)) != cluster_id ||
        wire::read_u64(fixed_span.subspan(16, 8)) != node_id ||
        wire::read_u64(fixed_span.subspan(24, 8)) != membership_fingerprint ||
        wire::read_u32(fixed_span.subspan(60, 4)) !=
            protocol::crc32(fixed_span.first(60))) {
      throw_corruption("invalid Raft snapshot header, version, or identity");
    }
    const auto index = wire::read_u64(fixed_span.subspan(32, 8));
    const auto term = wire::read_u64(fixed_span.subspan(40, 8));
    const auto payload_size = wire::read_u64(fixed_span.subspan(48, 8));
    if (index == 0 || index == std::numeric_limits<LogIndex>::max() ||
        term == 0 || payload_size > kMaxSnapshotPayloadSize ||
        payload_size >
            static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max()) ||
        static_cast<std::uint64_t>(status.st_size) !=
            kSnapshotHeaderSize + payload_size) {
      throw_corruption("invalid Raft snapshot boundary or payload size");
    }
    std::vector<std::byte> payload(static_cast<std::size_t>(payload_size));
    read_all(descriptor, payload);
    if (wire::read_u32(fixed_span.subspan(56, 4)) !=
        protocol::crc32(payload)) {
      throw_corruption("Raft snapshot payload checksum mismatch");
    }
    close_file(descriptor);
    return StateMachineSnapshot{.last_included_index = index,
                                .last_included_term = term,
                                .state_machine = std::move(payload)};
  } catch (...) {
    close_file(descriptor);
    throw;
  }
}

void SnapshotStore::write_atomic(
    const std::filesystem::path& directory, const std::uint64_t cluster_id,
    const NodeId node_id, const std::uint64_t membership_fingerprint,
    const StateMachineSnapshot& snapshot, SnapshotWriteHook hook) {
  if (snapshot.last_included_index == 0 ||
      snapshot.last_included_index == std::numeric_limits<LogIndex>::max() ||
      snapshot.last_included_term == 0 ||
      snapshot.state_machine.size() > kMaxSnapshotPayloadSize) {
    throw_invalid("invalid Raft snapshot boundary or payload size");
  }
  const std::lock_guard publish_lock(snapshot_publish_mutex);
  const auto current = load(directory, cluster_id, node_id,
                            membership_fingerprint);
  if (current.has_value() &&
      current->last_included_index > snapshot.last_included_index) {
    return;
  }
  if (current.has_value() &&
      current->last_included_index == snapshot.last_included_index) {
    if (*current != snapshot) {
      throw_invalid("snapshot boundary already has different contents");
    }
    return;
  }
  const auto temporary = temporary_path(directory);
  const auto published = published_path(directory);
  auto descriptor =
      ::open(temporary.c_str(),
             O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW, 0644);
  if (descriptor < 0) {
    throw_io("create temporary Raft snapshot");
  }
  bool renamed = false;
  try {
    const auto fixed = header(cluster_id, node_id, membership_fingerprint,
                              snapshot);
    write_all(descriptor, fixed);
    write_all(descriptor, snapshot.state_machine);
    if (hook) {
      hook(SnapshotWritePoint::after_write);
    }
    sync_file(descriptor);
    close_file(std::exchange(descriptor, -1));
    if (hook) {
      hook(SnapshotWritePoint::after_file_sync);
    }
    while (::rename(temporary.c_str(), published.c_str()) != 0) {
      if (errno != EINTR) {
        throw_io("publish Raft snapshot");
      }
    }
    renamed = true;
    if (hook) {
      hook(SnapshotWritePoint::after_rename);
    }
    sync_directory(directory);
    if (hook) {
      hook(SnapshotWritePoint::after_directory_sync);
    }
  } catch (...) {
    close_file(descriptor);
    if (!renamed) {
      static_cast<void>(::unlink(temporary.c_str()));
    }
    throw;
  }
}

}  // namespace forgekv::raft
