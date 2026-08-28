#include "raft/raft_storage.h"

#include "protocol/checksum.h"
#include "protocol/wire.h"
#include "raft/snapshot_store.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace forgekv::raft {
namespace {

namespace wire = forgekv::protocol::wire;

constexpr std::uint32_t kHardMagic = 0x46524853U;
constexpr std::uint32_t kIdentityMagic = 0x46524944U;
constexpr std::uint32_t kLogMagic = 0x46524C47U;
constexpr std::uint32_t kLogRecordMagic = 0x46524C52U;
constexpr std::uint16_t kFormatVersion = 2U;
constexpr std::uint16_t kLogFormatVersion = 3U;
constexpr std::size_t kHardStateSize = 60U;
constexpr std::size_t kIdentitySize = 40U;
constexpr std::size_t kLogHeaderSize = 52U;
constexpr std::size_t kLogRecordHeaderSize = 32U;
constexpr std::size_t kEntryHeaderSize = 24U;

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

void write_all(const int descriptor, const std::span<const std::byte> bytes) {
  std::size_t completed = 0;
  while (completed < bytes.size()) {
    const auto result =
        ::write(descriptor, bytes.data() + completed, bytes.size() - completed);
    if (result > 0) {
      completed += static_cast<std::size_t>(result);
    } else if (result < 0 && errno == EINTR) {
      continue;
    } else {
      if (result == 0) {
        errno = EIO;
      }
      throw_io("write Raft state");
    }
  }
}

std::size_t read_at(const int descriptor,
                    const std::span<std::byte> destination,
                    const std::uint64_t offset) {
  if (offset >
      static_cast<std::uint64_t>(std::numeric_limits<off_t>::max())) {
    throw RaftStorageError(RaftStorageErrorCode::io,
                           "Raft storage offset exceeds platform limit");
  }
  std::size_t completed = 0;
  while (completed < destination.size()) {
    const auto result = ::pread(
        descriptor, destination.data() + completed, destination.size() - completed,
        static_cast<off_t>(offset + completed));
    if (result > 0) {
      completed += static_cast<std::size_t>(result);
    } else if (result == 0) {
      break;
    } else if (errno != EINTR) {
      throw_io("read Raft state");
    }
  }
  return completed;
}

void sync_file(const int descriptor) {
  while (::fdatasync(descriptor) != 0) {
    if (errno != EINTR) {
      throw_io("sync Raft state");
    }
  }
}

void close_file(const int descriptor) noexcept {
  if (descriptor >= 0) {
    static_cast<void>(::close(descriptor));
  }
}

void truncate_file(const int descriptor, const std::uint64_t size) {
  if (size > static_cast<std::uint64_t>(
                 std::numeric_limits<off_t>::max())) {
    throw_corruption("Raft journal size exceeds platform limit");
  }
  while (::ftruncate(descriptor, static_cast<off_t>(size)) != 0) {
    if (errno != EINTR) {
      throw_io("truncate incomplete Raft journal tail");
    }
  }
}

void sync_directory(const std::filesystem::path& directory) {
  const auto descriptor =
      ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (descriptor < 0) {
    throw_io("open Raft data directory for sync");
  }
  try {
    while (::fsync(descriptor) != 0) {
      if (errno != EINTR) {
        throw_io("sync Raft data directory");
      }
    }
  } catch (...) {
    close_file(descriptor);
    throw;
  }
  close_file(descriptor);
}

void rename_file(const std::filesystem::path& source,
                 const std::filesystem::path& destination) {
  while (::rename(source.c_str(), destination.c_str()) != 0) {
    if (errno != EINTR) {
      throw_io("publish Raft state");
    }
  }
}

void unlink_file(const std::filesystem::path& path) noexcept {
  if (!path.empty()) {
    static_cast<void>(::unlink(path.c_str()));
  }
}

std::pair<int, std::filesystem::path> open_temporary_file(
    const std::filesystem::path& directory, const std::string& purpose) {
  for (std::uint64_t attempt = 0; attempt < 1'000; ++attempt) {
    const auto path = directory /
                      ("." + purpose + ".tmp." +
                       std::to_string(static_cast<std::uint64_t>(::getpid())) +
                       "." + std::to_string(attempt));
    const auto descriptor =
        ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
    if (descriptor >= 0) {
      return {descriptor, path};
    }
    if (errno != EEXIST) {
      throw_io("create temporary Raft state");
    }
  }
  throw RaftStorageError(RaftStorageErrorCode::io,
                         "cannot allocate temporary Raft state file");
}

void atomically_create_file(const std::filesystem::path& directory,
                            const std::filesystem::path& destination,
                            const std::span<const std::byte> contents,
                            const RaftStorageSyncHook& hook = {}) {
  auto [descriptor, temporary] =
      open_temporary_file(directory, destination.filename().string());
  try {
    write_all(descriptor, contents);
    if (hook) {
      hook(RaftStorageSyncPoint::after_write);
    }
    sync_file(descriptor);
    if (hook) {
      hook(RaftStorageSyncPoint::after_file_sync);
    }
    close_file(std::exchange(descriptor, -1));
    rename_file(temporary, destination);
    temporary.clear();
    if (hook) {
      hook(RaftStorageSyncPoint::after_rename);
    }
    sync_directory(directory);
    if (hook) {
      hook(RaftStorageSyncPoint::after_directory_sync);
    }
  } catch (...) {
    close_file(descriptor);
    unlink_file(temporary);
    throw;
  }
}

std::uint32_t checksum_without_tail(
    const std::span<const std::byte> bytes) {
  return protocol::crc32(bytes.first(bytes.size() - 4));
}

std::uint32_t log_record_checksum(
    const std::span<const std::byte> bytes) {
  protocol::Crc32 checksum;
  checksum.update(bytes.first(28));
  checksum.update(bytes.subspan(kLogRecordHeaderSize));
  return checksum.value();
}

struct HardRecord final {
  std::uint64_t generation{};
  Term term{};
  std::optional<NodeId> voted_for;
};

std::vector<std::byte> encode_hard_state(const std::uint64_t cluster_id,
                                         const NodeId node_id,
                                         const HardRecord& record) {
  std::vector<std::byte> bytes(kHardStateSize);
  wire::write_u32(std::span{bytes}.subspan(0, 4), kHardMagic);
  write_u16(std::span{bytes}.subspan(4, 2), kFormatVersion);
  write_u16(std::span{bytes}.subspan(6, 2),
            record.voted_for.has_value() ? 1U : 0U);
  wire::write_u32(std::span{bytes}.subspan(8, 4),
                  static_cast<std::uint32_t>(kHardStateSize));
  wire::write_u64(std::span{bytes}.subspan(16, 8), cluster_id);
  wire::write_u64(std::span{bytes}.subspan(24, 8), node_id);
  wire::write_u64(std::span{bytes}.subspan(32, 8), record.generation);
  wire::write_u64(std::span{bytes}.subspan(40, 8), record.term);
  wire::write_u64(std::span{bytes}.subspan(48, 8),
                  record.voted_for.value_or(0));
  wire::write_u32(std::span{bytes}.subspan(56, 4),
                  checksum_without_tail(bytes));
  return bytes;
}

std::vector<std::byte> encode_identity(
    const std::uint64_t cluster_id, const NodeId node_id,
    const std::uint64_t membership_fingerprint) {
  std::vector<std::byte> bytes(kIdentitySize);
  wire::write_u32(std::span{bytes}.subspan(0, 4), kIdentityMagic);
  write_u16(std::span{bytes}.subspan(4, 2), kFormatVersion);
  write_u16(std::span{bytes}.subspan(6, 2),
            static_cast<std::uint16_t>(kIdentitySize));
  wire::write_u64(std::span{bytes}.subspan(8, 8), cluster_id);
  wire::write_u64(std::span{bytes}.subspan(16, 8), node_id);
  wire::write_u64(std::span{bytes}.subspan(24, 8), membership_fingerprint);
  wire::write_u32(std::span{bytes}.subspan(36, 4),
                  checksum_without_tail(bytes));
  return bytes;
}

void verify_identity(const std::span<const std::byte> bytes,
                     const std::uint64_t cluster_id, const NodeId node_id,
                     const std::uint64_t membership_fingerprint) {
  if (bytes.size() != kIdentitySize ||
      wire::read_u32(bytes.subspan(0, 4)) != kIdentityMagic ||
      read_u16(bytes.subspan(4, 2)) != kFormatVersion ||
      read_u16(bytes.subspan(6, 2)) != kIdentitySize ||
      wire::read_u64(bytes.subspan(8, 8)) != cluster_id ||
      wire::read_u64(bytes.subspan(16, 8)) != node_id ||
      wire::read_u64(bytes.subspan(24, 8)) != membership_fingerprint ||
      wire::read_u32(bytes.subspan(32, 4)) != 0 ||
      wire::read_u32(bytes.subspan(36, 4)) !=
          checksum_without_tail(bytes)) {
    throw_corruption("invalid Raft initialized identity marker");
  }
}

std::optional<HardRecord> decode_hard_state(
    const std::span<const std::byte> bytes, const std::uint64_t cluster_id,
    const NodeId node_id) {
  if (bytes.size() != kHardStateSize ||
      wire::read_u32(bytes.subspan(0, 4)) != kHardMagic ||
      read_u16(bytes.subspan(4, 2)) != kFormatVersion ||
      wire::read_u32(bytes.subspan(8, 4)) != kHardStateSize ||
      wire::read_u32(bytes.subspan(12, 4)) != 0 ||
      wire::read_u64(bytes.subspan(16, 8)) != cluster_id ||
      wire::read_u64(bytes.subspan(24, 8)) != node_id ||
      wire::read_u32(bytes.subspan(56, 4)) !=
          checksum_without_tail(bytes)) {
    return std::nullopt;
  }
  const auto flags = read_u16(bytes.subspan(6, 2));
  const auto generation = wire::read_u64(bytes.subspan(32, 8));
  const auto term = wire::read_u64(bytes.subspan(40, 8));
  const auto vote = wire::read_u64(bytes.subspan(48, 8));
  if (flags > 1 || generation == 0 || (flags == 0 && vote != 0) ||
      (flags == 1 && vote == 0) || (term == 0 && flags == 1)) {
    return std::nullopt;
  }
  return HardRecord{generation, term,
                    flags == 1 ? std::optional<NodeId>{vote} : std::nullopt};
}

std::vector<std::byte> encode_log_header(
    const std::uint64_t cluster_id, const NodeId node_id,
    const std::uint64_t membership_fingerprint,
    const std::optional<StateMachineSnapshot>& required_snapshot =
        std::nullopt) {
  std::vector<std::byte> bytes(kLogHeaderSize);
  wire::write_u32(std::span{bytes}.subspan(0, 4), kLogMagic);
  write_u16(std::span{bytes}.subspan(4, 2), kLogFormatVersion);
  write_u16(std::span{bytes}.subspan(6, 2),
            static_cast<std::uint16_t>(kLogHeaderSize));
  wire::write_u64(std::span{bytes}.subspan(8, 8), cluster_id);
  wire::write_u64(std::span{bytes}.subspan(16, 8), node_id);
  wire::write_u64(std::span{bytes}.subspan(24, 8), membership_fingerprint);
  wire::write_u64(std::span{bytes}.subspan(32, 8),
                  required_snapshot.has_value()
                      ? required_snapshot->last_included_index
                      : 0);
  wire::write_u64(std::span{bytes}.subspan(40, 8),
                  required_snapshot.has_value()
                      ? required_snapshot->last_included_term
                      : 0);
  wire::write_u32(std::span{bytes}.subspan(48, 4),
                  checksum_without_tail(bytes));
  return bytes;
}

std::optional<std::pair<LogIndex, Term>> verify_log_header(
    const std::span<const std::byte> bytes, const std::uint64_t cluster_id,
    const NodeId node_id, const std::uint64_t membership_fingerprint) {
  const auto required_index = wire::read_u64(bytes.subspan(32, 8));
  const auto required_term = wire::read_u64(bytes.subspan(40, 8));
  if (bytes.size() != kLogHeaderSize ||
      wire::read_u32(bytes.subspan(0, 4)) != kLogMagic ||
      read_u16(bytes.subspan(4, 2)) != kLogFormatVersion ||
      read_u16(bytes.subspan(6, 2)) != kLogHeaderSize ||
      wire::read_u64(bytes.subspan(8, 8)) != cluster_id ||
      wire::read_u64(bytes.subspan(16, 8)) != node_id ||
      wire::read_u64(bytes.subspan(24, 8)) != membership_fingerprint ||
      wire::read_u32(bytes.subspan(48, 4)) != checksum_without_tail(bytes) ||
      ((required_index == 0) != (required_term == 0))) {
    throw_corruption("invalid Raft journal header or node identity");
  }
  if (required_index == 0) {
    return std::nullopt;
  }
  return std::pair{required_index, required_term};
}

RaftPersistentState apply_log_update(RaftPersistentState state,
                                     const PersistLog& update,
                                     const RaftStorageErrorCode error_code) {
  const auto reject = [error_code](const std::string& message) -> void {
    throw RaftStorageError(error_code, message);
  };
  const auto base_index = state.snapshot.has_value()
                              ? state.snapshot->last_included_index
                              : LogIndex{0};
  const auto base_term = state.snapshot.has_value()
                             ? state.snapshot->last_included_term
                             : Term{0};
  const auto last_index = base_index + state.log.size();
  if (update.from_index == 0 ||
      (update.from_index > base_index && update.from_index > last_index + 1) ||
      update.entries.size() > kMaxRaftLogEntriesPerRecord) {
    reject("invalid Raft log suffix boundary or entry count");
  }
  Term previous_term = 0;
  if (update.from_index == base_index + 1) {
    previous_term = base_term;
  } else if (update.from_index > base_index + 1) {
    previous_term = state.log[static_cast<std::size_t>(
                                  update.from_index - base_index - 2)]
                        .term;
  }
  LogIndex expected_index = update.from_index;
  std::size_t encoded_size = kLogRecordHeaderSize;
  for (const auto& entry : update.entries) {
    if (entry.index != expected_index || entry.term == 0 ||
        entry.term < previous_term || entry.term > state.current_term ||
        (entry.kind != EntryKind::command && entry.kind != EntryKind::no_op) ||
        entry.command.size() > kMaxRaftCommandSize ||
        (entry.kind == EntryKind::no_op && !entry.command.empty())) {
      reject("invalid Raft log entry");
    }
    if (encoded_size > kMaxRaftLogRecordSize - kEntryHeaderSize ||
        entry.command.size() >
            kMaxRaftLogRecordSize - encoded_size - kEntryHeaderSize) {
      reject("Raft log transaction exceeds maximum size");
    }
    encoded_size += kEntryHeaderSize + entry.command.size();
    if (entry.index == base_index && entry.term != base_term) {
      reject("Raft log record conflicts with snapshot boundary");
    }
    ++expected_index;
    previous_term = entry.term;
  }
  if (update.entries.empty() || update.entries.back().index <= base_index) {
    return state;
  }
  const auto retained_from = std::max(update.from_index, base_index + 1);
  if (retained_from > last_index + 1) {
    reject("invalid Raft log suffix after snapshot boundary");
  }
  const auto skip = static_cast<std::size_t>(retained_from - update.from_index);
  state.log.resize(static_cast<std::size_t>(retained_from - base_index - 1));
  state.log.insert(state.log.end(),
                   update.entries.begin() + static_cast<std::ptrdiff_t>(skip),
                   update.entries.end());
  return state;
}

std::vector<std::byte> encode_log_update(const PersistLog& update) {
  std::size_t size = kLogRecordHeaderSize;
  for (const auto& entry : update.entries) {
    size += kEntryHeaderSize + entry.command.size();
  }
  std::vector<std::byte> bytes(size);
  wire::write_u32(std::span{bytes}.subspan(0, 4), kLogRecordMagic);
  write_u16(std::span{bytes}.subspan(4, 2), kFormatVersion);
  write_u16(std::span{bytes}.subspan(6, 2),
            static_cast<std::uint16_t>(kLogRecordHeaderSize));
  wire::write_u32(std::span{bytes}.subspan(8, 4),
                  static_cast<std::uint32_t>(size));
  wire::write_u32(std::span{bytes}.subspan(12, 4),
                  static_cast<std::uint32_t>(update.entries.size()));
  wire::write_u64(std::span{bytes}.subspan(16, 8), update.from_index);
  wire::write_u32(std::span{bytes}.subspan(24, 4),
                  protocol::crc32(std::span<const std::byte>{bytes}.first(24)));
  std::size_t offset = kLogRecordHeaderSize;
  for (const auto& entry : update.entries) {
    wire::write_u64(std::span{bytes}.subspan(offset, 8), entry.index);
    wire::write_u64(std::span{bytes}.subspan(offset + 8, 8), entry.term);
    bytes[offset + 16] = static_cast<std::byte>(entry.kind);
    wire::write_u32(std::span{bytes}.subspan(offset + 20, 4),
                    static_cast<std::uint32_t>(entry.command.size()));
    std::ranges::copy(entry.command,
                      bytes.begin() + static_cast<std::ptrdiff_t>(
                                          offset + kEntryHeaderSize));
    offset += kEntryHeaderSize + entry.command.size();
  }
  wire::write_u32(std::span{bytes}.subspan(28, 4),
                  log_record_checksum(bytes));
  return bytes;
}

PersistLog decode_log_update(const std::span<const std::byte> bytes) {
  if (bytes.size() < kLogRecordHeaderSize ||
      wire::read_u32(bytes.subspan(0, 4)) != kLogRecordMagic ||
      read_u16(bytes.subspan(4, 2)) != kFormatVersion ||
      read_u16(bytes.subspan(6, 2)) != kLogRecordHeaderSize ||
      wire::read_u32(bytes.subspan(8, 4)) != bytes.size() ||
      wire::read_u32(bytes.subspan(24, 4)) !=
          protocol::crc32(bytes.first(24)) ||
      wire::read_u32(bytes.subspan(28, 4)) != log_record_checksum(bytes)) {
    throw_corruption("invalid Raft log transaction header or checksum");
  }
  const auto entry_count = wire::read_u32(bytes.subspan(12, 4));
  if (entry_count > kMaxRaftLogEntriesPerRecord) {
    throw_corruption("Raft log transaction entry count exceeds limit");
  }
  PersistLog update{wire::read_u64(bytes.subspan(16, 8)), {}};
  update.entries.reserve(entry_count);
  std::size_t offset = kLogRecordHeaderSize;
  for (std::uint32_t count = 0; count < entry_count; ++count) {
    if (bytes.size() - offset < kEntryHeaderSize) {
      throw_corruption("truncated Raft log entry header");
    }
    const auto payload_size = wire::read_u32(bytes.subspan(offset + 20, 4));
    if (payload_size > kMaxRaftCommandSize ||
        payload_size > bytes.size() - offset - kEntryHeaderSize ||
        bytes[offset + 17] != std::byte{0} ||
        bytes[offset + 18] != std::byte{0} ||
        bytes[offset + 19] != std::byte{0}) {
      throw_corruption("invalid Raft log entry length or reserved fields");
    }
    const auto kind = static_cast<EntryKind>(
        std::to_integer<std::uint8_t>(bytes[offset + 16]));
    const auto payload = bytes.subspan(offset + kEntryHeaderSize, payload_size);
    update.entries.push_back(LogEntry{
        wire::read_u64(bytes.subspan(offset, 8)),
        wire::read_u64(bytes.subspan(offset + 8, 8)), kind,
        std::vector<std::byte>(payload.begin(), payload.end())});
    offset += kEntryHeaderSize + payload_size;
  }
  if (offset != bytes.size()) {
    throw_corruption("Raft log transaction has trailing bytes");
  }
  return update;
}

void verify_log_record_header(const std::span<const std::byte> bytes) {
  if (bytes.size() != kLogRecordHeaderSize ||
      wire::read_u32(bytes.subspan(0, 4)) != kLogRecordMagic ||
      read_u16(bytes.subspan(4, 2)) != kFormatVersion ||
      read_u16(bytes.subspan(6, 2)) != kLogRecordHeaderSize ||
      wire::read_u32(bytes.subspan(24, 4)) !=
          protocol::crc32(bytes.first(24))) {
    throw_corruption("invalid Raft journal fixed record header");
  }
}

}  // namespace

RaftStorageError::RaftStorageError(const RaftStorageErrorCode code,
                                   const std::string& message)
    : std::runtime_error(message), code_(code) {}

RaftStorageErrorCode RaftStorageError::code() const noexcept { return code_; }

struct RaftStorage::Impl final {
  enum class PendingKind { none, hard_state, log };

  std::filesystem::path directory;
  std::uint64_t cluster_id{};
  NodeId node_id{};
  std::uint64_t membership_fingerprint{};
  int log_descriptor{-1};
  int lock_descriptor{-1};
  int pending_descriptor{-1};
  std::filesystem::path pending_temporary_path;
  std::filesystem::path pending_final_path;
  PendingKind pending_kind{PendingKind::none};
  std::uint64_t hard_generation{};
  RaftPersistentState state;
  RaftPersistentState pending_state;
  RaftStorageSyncHook sync_hook;

  ~Impl() {
    close_file(pending_descriptor);
    unlink_file(pending_temporary_path);
    close_file(log_descriptor);
    close_file(lock_descriptor);
  }
};

namespace {

bool path_exists(const std::filesystem::path& path) {
  std::error_code error;
  const bool exists = std::filesystem::exists(path, error);
  if (error) {
    throw RaftStorageError(RaftStorageErrorCode::io,
                           "inspect Raft state path: " + error.message());
  }
  return exists;
}

void read_and_verify_identity(const std::filesystem::path& path,
                              const std::uint64_t cluster_id,
                              const NodeId node_id,
                              const std::uint64_t membership_fingerprint) {
  const auto descriptor = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (descriptor < 0) {
    throw_io("open Raft initialized identity marker");
  }
  std::array<std::byte, kIdentitySize> bytes{};
  std::size_t read = 0;
  struct stat metadata {};
  try {
    read = read_at(descriptor, bytes, 0);
    if (::fstat(descriptor, &metadata) != 0) {
      throw_io("stat Raft initialized identity marker");
    }
  } catch (...) {
    close_file(descriptor);
    throw;
  }
  close_file(descriptor);
  if (metadata.st_size == 32) {
    throw_corruption(
        "Raft format version 1 requires offline migration or data wipe");
  }
  if (read != bytes.size() || metadata.st_size != kIdentitySize) {
    throw_corruption("truncated Raft initialized identity marker");
  }
  verify_identity(bytes, cluster_id, node_id, membership_fingerprint);
}

bool is_recognized_temporary_name(const std::string_view name) {
  constexpr std::array prefixes{
      std::string_view{".hard-state.A.tmp."},
      std::string_view{".hard-state.B.tmp."},
      std::string_view{".raft-log.wal.tmp."},
      std::string_view{".IDENTITY.tmp."},
  };
  for (const auto prefix : prefixes) {
    if (!name.starts_with(prefix)) {
      continue;
    }
    const auto suffix = name.substr(prefix.size());
    const auto separator = suffix.find('.');
    if (separator == std::string_view::npos || separator == 0 ||
        separator + 1 == suffix.size()) {
      return false;
    }
    const auto digits = [](const std::string_view value) {
      return std::ranges::all_of(value, [](const char character) {
        return character >= '0' && character <= '9';
      });
    };
    return digits(suffix.substr(0, separator)) &&
           digits(suffix.substr(separator + 1));
  }
  return false;
}

void remove_stale_temporary_files(const std::filesystem::path& directory) {
  std::error_code error;
  bool removed = false;
  for (std::filesystem::directory_iterator iterator(directory, error), end;
       iterator != end && !error; iterator.increment(error)) {
    if (!iterator->is_regular_file(error)) {
      if (error) {
        break;
      }
      continue;
    }
    if (!is_recognized_temporary_name(
            iterator->path().filename().string())) {
      continue;
    }
    removed = std::filesystem::remove(iterator->path(), error) || removed;
    if (error) {
      break;
    }
  }
  if (error) {
    throw RaftStorageError(RaftStorageErrorCode::io,
                           "clean stale Raft temporary files: " +
                               error.message());
  }
  if (removed) {
    sync_directory(directory);
  }
}

std::optional<HardRecord> read_hard_slot(const std::filesystem::path& path,
                                         const std::uint64_t cluster_id,
                                         const NodeId node_id,
                                         bool& exists) {
  exists = path_exists(path);
  if (!exists) {
    return std::nullopt;
  }
  const auto descriptor = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (descriptor < 0) {
    throw_io("open Raft hard state");
  }
  std::array<std::byte, kHardStateSize> bytes{};
  std::size_t read = 0;
  struct stat metadata {};
  try {
    read = read_at(descriptor, bytes, 0);
    if (::fstat(descriptor, &metadata) != 0) {
      throw_io("stat Raft hard state");
    }
  } catch (...) {
    close_file(descriptor);
    throw;
  }
  close_file(descriptor);
  if (read != bytes.size() || metadata.st_size != kHardStateSize) {
    return std::nullopt;
  }
  return decode_hard_state(bytes, cluster_id, node_id);
}

template <typename ImplType>
void recover_log(ImplType& impl) {
  struct stat metadata {};
  if (::fstat(impl.log_descriptor, &metadata) != 0) {
    throw_io("stat Raft journal");
  }
  if (metadata.st_size < 0) {
    throw_corruption("negative Raft journal size");
  }
  const auto file_size = static_cast<std::uint64_t>(metadata.st_size);
  if (file_size < kLogHeaderSize) {
    throw_corruption("truncated Raft journal header");
  }
  std::array<std::byte, kLogHeaderSize> header{};
  if (read_at(impl.log_descriptor, header, 0) != header.size()) {
    throw_corruption("cannot read Raft journal header");
  }
  const auto required_snapshot =
      verify_log_header(header, impl.cluster_id, impl.node_id,
                        impl.membership_fingerprint);
  const auto loaded_snapshot = impl.state.snapshot;
  auto replay_state = impl.state;
  replay_state.log.clear();
  if (required_snapshot.has_value()) {
    if (!loaded_snapshot.has_value() ||
        loaded_snapshot->last_included_index < required_snapshot->first ||
        (loaded_snapshot->last_included_index == required_snapshot->first &&
         loaded_snapshot->last_included_term != required_snapshot->second)) {
      throw_corruption("Raft journal requires a missing or older snapshot");
    }
    replay_state.snapshot = StateMachineSnapshot{
        .last_included_index = required_snapshot->first,
        .last_included_term = required_snapshot->second,
        .state_machine = {},
    };
  } else {
    replay_state.snapshot.reset();
  }

  std::uint64_t offset = kLogHeaderSize;
  while (offset < file_size) {
    if (file_size - offset < kLogRecordHeaderSize) {
      truncate_file(impl.log_descriptor, offset);
      sync_file(impl.log_descriptor);
      break;
    }
    std::array<std::byte, kLogRecordHeaderSize> record_header{};
    if (read_at(impl.log_descriptor, record_header, offset) !=
        record_header.size()) {
      truncate_file(impl.log_descriptor, offset);
      sync_file(impl.log_descriptor);
      break;
    }
    verify_log_record_header(record_header);
    const auto record_size = wire::read_u32(
        std::span<const std::byte>{record_header}.subspan(8, 4));
    if (record_size < kLogRecordHeaderSize ||
        record_size > kMaxRaftLogRecordSize) {
      throw_corruption("invalid Raft journal record size");
    }
    if (file_size - offset < record_size) {
      truncate_file(impl.log_descriptor, offset);
      sync_file(impl.log_descriptor);
      break;
    }
    std::vector<std::byte> bytes(record_size);
    if (read_at(impl.log_descriptor, bytes, offset) != bytes.size()) {
      throw_corruption("cannot read complete Raft journal record");
    }
    replay_state = apply_log_update(std::move(replay_state),
                                    decode_log_update(bytes),
                                    RaftStorageErrorCode::corruption);
    offset += record_size;
  }
  if (loaded_snapshot.has_value()) {
    const auto replay_base = required_snapshot.has_value()
                                 ? required_snapshot->first
                                 : LogIndex{0};
    const auto boundary = loaded_snapshot->last_included_index;
    const auto covered_entries = boundary - replay_base;
    const auto boundary_matches =
        covered_entries == 0
            ? required_snapshot.has_value() &&
                  required_snapshot->second ==
                      loaded_snapshot->last_included_term
            : covered_entries <= replay_state.log.size() &&
                  replay_state
                          .log[static_cast<std::size_t>(covered_entries - 1)]
                          .term == loaded_snapshot->last_included_term;
    if (!boundary_matches) {
      throw_corruption("full Raft journal conflicts with snapshot boundary");
    }
    replay_state.log.erase(
        replay_state.log.begin(),
        replay_state.log.begin() +
            static_cast<std::ptrdiff_t>(covered_entries));
    replay_state.snapshot = loaded_snapshot;
  }
  impl.state = std::move(replay_state);
  if (::lseek(impl.log_descriptor, 0, SEEK_END) < 0) {
    throw_io("seek Raft journal append position");
  }
}

}  // namespace

RaftStorage RaftStorage::open(const std::filesystem::path& directory,
                              const std::uint64_t cluster_id,
                              const NodeId node_id,
                              RaftStorageSyncHook sync_hook,
                              const std::uint64_t membership_fingerprint) {
  if (cluster_id == 0 || node_id == 0) {
    throw_invalid("Raft storage cluster and node IDs must be nonzero");
  }
  std::error_code error;
  if (!std::filesystem::exists(directory, error)) {
    if (error) {
      throw RaftStorageError(RaftStorageErrorCode::io,
                             "inspect Raft data directory: " + error.message());
    }
    auto parent = directory.parent_path();
    if (parent.empty()) {
      parent = std::filesystem::current_path(error);
    }
    if (error || !std::filesystem::is_directory(parent, error) || error ||
        !std::filesystem::create_directory(directory, error) || error) {
      throw RaftStorageError(RaftStorageErrorCode::io,
                             "create Raft data directory: " + error.message());
    }
    sync_directory(parent);
  } else if (error || !std::filesystem::is_directory(directory, error) ||
             error) {
    throw RaftStorageError(RaftStorageErrorCode::io,
                           "Raft data path is not a directory");
  }
  auto impl = std::make_unique<Impl>();
  impl->directory = directory;
  impl->cluster_id = cluster_id;
  impl->node_id = node_id;
  impl->membership_fingerprint = membership_fingerprint;
  impl->sync_hook = std::move(sync_hook);
  const auto lock_path = directory / "LOCK";
  impl->lock_descriptor =
      ::open(lock_path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
  if (impl->lock_descriptor < 0) {
    throw_io("open Raft data-directory lock");
  }
  while (::flock(impl->lock_descriptor, LOCK_EX | LOCK_NB) != 0) {
    if (errno == EINTR) {
      continue;
    }
    if (errno == EWOULDBLOCK || errno == EAGAIN) {
      throw RaftStorageError(RaftStorageErrorCode::io,
                             "Raft data directory is already in use");
    }
    throw_io("lock Raft data directory");
  }

  const auto identity_path = directory / "IDENTITY";
  const auto hard_a_path = directory / "hard-state.A";
  const auto hard_b_path = directory / "hard-state.B";
  const auto log_path = directory / "raft-log.wal";
  const bool identity_existed = path_exists(identity_path);
  const bool hard_a_existed = path_exists(hard_a_path);
  const bool hard_b_existed = path_exists(hard_b_path);
  const bool log_existed = path_exists(log_path);
  if (identity_existed) {
    read_and_verify_identity(identity_path, cluster_id, node_id,
                             membership_fingerprint);
    if (!hard_a_existed || !hard_b_existed || !log_existed) {
      throw_corruption("initialized Raft store is missing a durable artifact");
    }
  } else if (!hard_a_existed && !hard_b_existed && log_existed) {
    throw_corruption("Raft journal exists without hard state or identity");
  }
  if (!hard_a_existed && !hard_b_existed) {
    const auto baseline = encode_hard_state(
        cluster_id, node_id,
        HardRecord{.generation = 1, .term = 0, .voted_for = std::nullopt});
    atomically_create_file(directory, hard_a_path, baseline);
  }

  bool a_exists = false;
  bool b_exists = false;
  const auto a = read_hard_slot(hard_a_path, cluster_id, node_id, a_exists);
  auto b = read_hard_slot(hard_b_path, cluster_id, node_id, b_exists);
  if (!identity_existed && a.has_value() && a->generation == 1 &&
      a->term == 0 && !a->voted_for.has_value() && !b_exists) {
    const auto second_baseline = encode_hard_state(
        cluster_id, node_id,
        HardRecord{.generation = 2, .term = 0, .voted_for = std::nullopt});
    atomically_create_file(directory, hard_b_path, second_baseline);
    b = read_hard_slot(hard_b_path, cluster_id, node_id, b_exists);
  }
  if ((a_exists && !a.has_value()) || (b_exists && !b.has_value())) {
    throw_corruption("invalid Raft hard-state generation");
  }
  if (!a.has_value() || !b.has_value()) {
    throw_corruption("missing Raft hard-state generation");
  }
  if ((a.has_value() && a->generation % 2 != 1) ||
      (b.has_value() && b->generation % 2 != 0) ||
      (a.has_value() && b.has_value() &&
       (a->generation > b->generation
            ? a->generation - b->generation
            : b->generation - a->generation) != 1)) {
    throw_corruption("inconsistent Raft hard-state generations");
  }
  const auto selected = !a.has_value()   ? b
                        : !b.has_value() ? a
                        : a->generation >= b->generation ? a
                                                         : b;
  if (!selected.has_value()) {
    throw_corruption("Raft store has no recoverable hard state");
  }
  impl->hard_generation = selected->generation;
  impl->state.current_term = selected->term;
  impl->state.voted_for = selected->voted_for;
  impl->state.snapshot = SnapshotStore::load(
      directory, cluster_id, node_id, membership_fingerprint);
  if (impl->state.snapshot.has_value() &&
      impl->state.snapshot->last_included_term > impl->state.current_term) {
    throw_corruption("Raft snapshot term exceeds durable hard state");
  }

  if (!log_existed) {
    if (identity_existed || selected->generation != 2 || selected->term != 0 ||
        selected->voted_for.has_value()) {
      throw_corruption("Raft journal is missing from a non-pristine store");
    }
    const auto header =
        encode_log_header(cluster_id, node_id, membership_fingerprint);
    atomically_create_file(directory, log_path, header);
  }
  impl->log_descriptor =
      ::open(log_path.c_str(), O_RDWR | O_CLOEXEC | O_APPEND);
  if (impl->log_descriptor < 0) {
    throw_io("open Raft journal");
  }
  struct stat metadata {};
  if (::fstat(impl->log_descriptor, &metadata) != 0) {
    throw_io("stat Raft journal");
  }
  recover_log(*impl);
  if (!identity_existed &&
      (selected->generation != 2 || selected->term != 0 ||
       selected->voted_for.has_value() || !impl->state.log.empty())) {
    throw_corruption("non-pristine Raft store is missing its identity marker");
  }

  // Recovery may observe a complete append or rename from a process that died
  // before its final sync. Complete the durability of every accepted byte and
  // namespace entry before the recovered state can drive protocol output.
  sync_file(impl->log_descriptor);
  sync_directory(directory);
  if (!identity_existed) {
    const auto identity =
        encode_identity(cluster_id, node_id, membership_fingerprint);
    atomically_create_file(directory, identity_path, identity);
  }
  remove_stale_temporary_files(directory);
  return RaftStorage(std::move(impl));
}

RaftStorage::RaftStorage(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
RaftStorage::~RaftStorage() = default;
RaftStorage::RaftStorage(RaftStorage&&) noexcept = default;
RaftStorage& RaftStorage::operator=(RaftStorage&&) noexcept = default;

void RaftStorage::prepare(const PersistHardState& update) {
  if (!impl_ || impl_->log_descriptor < 0) {
    throw RaftStorageError(RaftStorageErrorCode::closed,
                           "prepare closed Raft storage");
  }
  if (impl_->pending_kind != Impl::PendingKind::none) {
    throw_invalid("Raft storage already has a pending update");
  }
  if (update.term < impl_->state.current_term ||
      (update.term == 0 && update.voted_for.has_value()) ||
      (update.voted_for.has_value() && *update.voted_for == 0) ||
      (update.term == impl_->state.current_term &&
       impl_->state.voted_for.has_value() &&
       update.voted_for != impl_->state.voted_for)) {
    throw_invalid("Raft hard-state term or vote regression");
  }
  for (const auto& entry : impl_->state.log) {
    if (entry.term > update.term) {
      throw_invalid("Raft hard state cannot move below a log term");
    }
  }

  if (impl_->hard_generation == std::numeric_limits<std::uint64_t>::max()) {
    throw_invalid("Raft hard-state generation exhausted");
  }
  const auto generation = impl_->hard_generation + 1;
  const auto filename = generation % 2 == 1 ? "hard-state.A" : "hard-state.B";
  impl_->pending_final_path = impl_->directory / filename;
  auto temporary = open_temporary_file(impl_->directory, filename);
  impl_->pending_descriptor = temporary.first;
  impl_->pending_temporary_path = std::move(temporary.second);
  try {
    write_all(impl_->pending_descriptor,
              encode_hard_state(impl_->cluster_id, impl_->node_id,
                                HardRecord{generation, update.term,
                                           update.voted_for}));
  } catch (...) {
    close_file(std::exchange(impl_->pending_descriptor, -1));
    unlink_file(impl_->pending_temporary_path);
    impl_->pending_temporary_path.clear();
    impl_->pending_final_path.clear();
    throw;
  }
  impl_->pending_state = impl_->state;
  impl_->pending_state.current_term = update.term;
  impl_->pending_state.voted_for = update.voted_for;
  impl_->pending_kind = Impl::PendingKind::hard_state;
}

void RaftStorage::prepare(const PersistLog& update) {
  if (!impl_ || impl_->log_descriptor < 0) {
    throw RaftStorageError(RaftStorageErrorCode::closed,
                           "prepare closed Raft storage");
  }
  if (impl_->pending_kind != Impl::PendingKind::none) {
    throw_invalid("Raft storage already has a pending update");
  }
  auto next = apply_log_update(impl_->state, update,
                               RaftStorageErrorCode::invalid_update);
  write_all(impl_->log_descriptor, encode_log_update(update));
  impl_->pending_state = std::move(next);
  impl_->pending_kind = Impl::PendingKind::log;
}

void RaftStorage::install_snapshot(const StateMachineSnapshot& snapshot,
                                   const bool snapshot_already_durable) {
  if (!impl_ || impl_->log_descriptor < 0) {
    throw RaftStorageError(RaftStorageErrorCode::closed,
                           "install snapshot on closed Raft storage");
  }
  if (impl_->pending_kind != Impl::PendingKind::none) {
    throw_invalid("Raft storage already has a pending update");
  }
  const auto old_base = impl_->state.snapshot.has_value()
                            ? impl_->state.snapshot->last_included_index
                            : LogIndex{0};
  if (snapshot.last_included_index ==
          std::numeric_limits<LogIndex>::max() ||
      snapshot.last_included_index <= old_base ||
      snapshot.last_included_term == 0 ||
      snapshot.last_included_term > impl_->state.current_term ||
      snapshot.state_machine.size() > kMaxSnapshotPayloadSize) {
    throw_invalid("invalid or regressing Raft snapshot");
  }

  std::vector<LogEntry> suffix;
  const auto old_last = old_base + impl_->state.log.size();
  if (snapshot.last_included_index <= old_last) {
    const auto boundary = static_cast<std::size_t>(
        snapshot.last_included_index - old_base - 1);
    if (impl_->state.log.at(boundary).term == snapshot.last_included_term) {
      suffix.assign(impl_->state.log.begin() +
                        static_cast<std::ptrdiff_t>(boundary + 1),
                    impl_->state.log.end());
    }
  }

  if (!snapshot_already_durable) {
    SnapshotStore::write_atomic(
        impl_->directory, impl_->cluster_id, impl_->node_id,
        impl_->membership_fingerprint, snapshot,
        [this](const SnapshotWritePoint point) {
          if (!impl_->sync_hook) {
            return;
          }
          if (point == SnapshotWritePoint::after_file_sync) {
            impl_->sync_hook(RaftStorageSyncPoint::after_file_sync);
          } else if (point == SnapshotWritePoint::after_rename) {
            impl_->sync_hook(RaftStorageSyncPoint::after_rename);
          }
        });
  } else {
    const auto durable = SnapshotStore::load(
        impl_->directory, impl_->cluster_id, impl_->node_id,
        impl_->membership_fingerprint);
    if (!durable.has_value() || *durable != snapshot) {
      throw_invalid("prewritten snapshot does not match compact request");
    }
  }

  std::vector<std::byte> journal = encode_log_header(
      impl_->cluster_id, impl_->node_id, impl_->membership_fingerprint,
      snapshot);
  std::size_t suffix_offset = 0;
  while (suffix_offset < suffix.size()) {
    std::size_t batch_size = 0;
    std::size_t encoded_size = kLogRecordHeaderSize;
    while (suffix_offset + batch_size < suffix.size() &&
           batch_size < kMaxRaftLogEntriesPerRecord) {
      const auto entry_size =
          kEntryHeaderSize + suffix[suffix_offset + batch_size].command.size();
      if (batch_size != 0 &&
          entry_size > kMaxRaftLogRecordSize - encoded_size) {
        break;
      }
      encoded_size += entry_size;
      ++batch_size;
    }
    std::vector<LogEntry> batch{
        suffix.begin() + static_cast<std::ptrdiff_t>(suffix_offset),
        suffix.begin() +
            static_cast<std::ptrdiff_t>(suffix_offset + batch_size)};
    auto record = encode_log_update(PersistLog{
        .from_index = snapshot.last_included_index + 1 + suffix_offset,
        .entries = std::move(batch),
    });
    journal.insert(journal.end(), record.begin(), record.end());
    suffix_offset += batch_size;
  }
  const auto log_path = impl_->directory / "raft-log.wal";
  atomically_create_file(impl_->directory, log_path, journal,
                         impl_->sync_hook);
  close_file(std::exchange(impl_->log_descriptor, -1));
  impl_->log_descriptor =
      ::open(log_path.c_str(), O_RDWR | O_CLOEXEC | O_APPEND);
  if (impl_->log_descriptor < 0) {
    throw_io("reopen compacted Raft journal");
  }
  impl_->state.snapshot = snapshot;
  impl_->state.log = std::move(suffix);
}

void RaftStorage::sync() {
  if (!impl_ || impl_->log_descriptor < 0) {
    throw RaftStorageError(RaftStorageErrorCode::closed,
                           "sync closed Raft storage");
  }
  if (impl_->pending_kind == Impl::PendingKind::none) {
    throw_invalid("Raft storage has no pending update to sync");
  }
  if (impl_->pending_kind == Impl::PendingKind::hard_state) {
    sync_file(impl_->pending_descriptor);
    if (impl_->sync_hook) {
      impl_->sync_hook(RaftStorageSyncPoint::after_file_sync);
    }
    close_file(std::exchange(impl_->pending_descriptor, -1));
    rename_file(impl_->pending_temporary_path, impl_->pending_final_path);
    if (impl_->sync_hook) {
      impl_->sync_hook(RaftStorageSyncPoint::after_rename);
    }
    impl_->pending_temporary_path.clear();
    impl_->pending_final_path.clear();
    sync_directory(impl_->directory);
    ++impl_->hard_generation;
  } else {
    sync_file(impl_->log_descriptor);
    if (impl_->sync_hook) {
      impl_->sync_hook(RaftStorageSyncPoint::after_file_sync);
    }
  }
  impl_->state = std::move(impl_->pending_state);
  impl_->pending_state = {};
  impl_->pending_kind = Impl::PendingKind::none;
}

void RaftStorage::close() noexcept {
  if (impl_) {
    close_file(std::exchange(impl_->pending_descriptor, -1));
    unlink_file(impl_->pending_temporary_path);
    impl_->pending_temporary_path.clear();
    impl_->pending_final_path.clear();
    close_file(std::exchange(impl_->log_descriptor, -1));
    close_file(std::exchange(impl_->lock_descriptor, -1));
    impl_->pending_kind = Impl::PendingKind::none;
  }
}

const RaftPersistentState& RaftStorage::state() const noexcept {
  static const RaftPersistentState empty;
  return impl_ ? impl_->state : empty;
}

bool RaftStorage::has_pending_update() const noexcept {
  return impl_ && impl_->pending_kind != Impl::PendingKind::none;
}

}  // namespace forgekv::raft
