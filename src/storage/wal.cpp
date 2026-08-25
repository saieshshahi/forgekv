#include "storage/wal.h"

#include <array>
#include <cerrno>
#include <cstring>
#include <limits>
#include <span>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "protocol/wire.h"

namespace forgekv::storage {
namespace {

[[noreturn]] void throw_io(const std::string& action) {
  throw StorageError(StorageErrorCode::io,
                     action + ": " + std::string(std::strerror(errno)));
}

std::size_t read_at(const int descriptor, const std::span<std::byte> destination,
                    const std::uint64_t offset) {
  std::size_t completed = 0U;
  while (completed < destination.size()) {
    const auto result = ::pread(
        descriptor, destination.data() + completed, destination.size() - completed,
        static_cast<off_t>(offset + completed));
    if (result > 0) {
      completed += static_cast<std::size_t>(result);
      continue;
    }
    if (result == 0) {
      break;
    }
    if (errno == EINTR) {
      continue;
    }
    throw_io("read WAL");
  }
  return completed;
}

void write_all(const int descriptor, const std::span<const std::byte> bytes) {
  std::size_t completed = 0U;
  while (completed < bytes.size()) {
    const auto result =
        ::write(descriptor, bytes.data() + completed, bytes.size() - completed);
    if (result > 0) {
      completed += static_cast<std::size_t>(result);
      continue;
    }
    if (result < 0 && errno == EINTR) {
      continue;
    }
    if (result == 0) {
      errno = EIO;
    }
    throw_io("append WAL");
  }
}

void truncate_to(const int descriptor, const std::uint64_t size) {
  if (size > static_cast<std::uint64_t>(std::numeric_limits<off_t>::max())) {
    throw StorageError(StorageErrorCode::io, "WAL offset exceeds platform limit");
  }
  while (::ftruncate(descriptor, static_cast<off_t>(size)) != 0) {
    if (errno != EINTR) {
      throw_io("truncate incomplete WAL tail");
    }
  }
}

}  // namespace

StorageError::StorageError(const StorageErrorCode code, const std::string& message)
    : std::runtime_error(message), code_(code) {}

StorageErrorCode StorageError::code() const noexcept { return code_; }

Wal Wal::open(const std::filesystem::path& path) {
  const auto descriptor =
      ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC | O_APPEND, 0644);
  if (descriptor < 0) {
    throw_io("open WAL " + path.string());
  }

  Wal wal(descriptor, path);
  try {
    wal.recover([](const WalRecord&) {});
  } catch (...) {
    wal.close();
    throw;
  }
  return wal;
}

Wal::Wal(const int descriptor, std::filesystem::path path)
    : descriptor_(descriptor), path_(std::move(path)) {}

Wal::Wal(Wal&& other) noexcept
    : descriptor_(std::exchange(other.descriptor_, -1)),
      path_(std::move(other.path_)),
      last_lsn_(std::exchange(other.last_lsn_, 0U)),
      durable_lsn_(std::exchange(other.durable_lsn_, 0U)) {}

Wal& Wal::operator=(Wal&& other) noexcept {
  if (this != &other) {
    close();
    descriptor_ = std::exchange(other.descriptor_, -1);
    path_ = std::move(other.path_);
    last_lsn_ = std::exchange(other.last_lsn_, 0U);
    durable_lsn_ = std::exchange(other.durable_lsn_, 0U);
  }
  return *this;
}

Wal::~Wal() { close(); }

void Wal::append(const WalRecord& record) {
  if (descriptor_ < 0) {
    throw StorageError(StorageErrorCode::closed, "append to closed WAL");
  }
  if (record.lsn != last_lsn_ + 1U) {
    throw StorageError(StorageErrorCode::invalid_lsn,
                       "WAL LSN must be the next gap-free value");
  }
  const auto encoded = encode_record(record);
  write_all(descriptor_, encoded);
  last_lsn_ = record.lsn;
}

void Wal::sync() {
  if (descriptor_ < 0) {
    throw StorageError(StorageErrorCode::closed, "sync closed WAL");
  }
  while (::fdatasync(descriptor_) != 0) {
    if (errno != EINTR) {
      throw_io("sync WAL");
    }
  }
  durable_lsn_ = last_lsn_;
}

void Wal::recover(const ReplayCallback& callback) {
  if (descriptor_ < 0) {
    throw StorageError(StorageErrorCode::closed, "recover closed WAL");
  }

  struct stat metadata {};
  if (::fstat(descriptor_, &metadata) != 0) {
    throw_io("stat WAL");
  }
  if (metadata.st_size < 0) {
    throw StorageError(StorageErrorCode::io, "WAL has a negative size");
  }
  const auto file_size = static_cast<std::uint64_t>(metadata.st_size);
  std::uint64_t offset = 0U;
  std::uint64_t recovered_lsn = 0U;

  while (offset < file_size) {
    const auto remaining = file_size - offset;
    if (remaining < kWalHeaderSize) {
      truncate_to(descriptor_, offset);
      break;
    }

    std::array<std::byte, kWalHeaderSize> header{};
    if (read_at(descriptor_, header, offset) != header.size()) {
      truncate_to(descriptor_, offset);
      break;
    }
    const auto header_result = decode_record(header);
    if (header_result.status == DecodeStatus::corrupt) {
      throw StorageError(StorageErrorCode::corruption,
                         "WAL corruption at offset " + std::to_string(offset) +
                             ": " + header_result.error);
    }

    const auto record_size = protocol::wire::read_u32(
        std::span<const std::byte>{header}.subspan(8U, 4U));
    if (record_size < kWalHeaderSize || record_size > kMaxWalRecordSize) {
      throw StorageError(StorageErrorCode::corruption,
                         "invalid WAL record size at offset " +
                             std::to_string(offset));
    }
    if (remaining < record_size) {
      truncate_to(descriptor_, offset);
      break;
    }

    std::vector<std::byte> bytes(record_size);
    if (read_at(descriptor_, bytes, offset) != bytes.size()) {
      truncate_to(descriptor_, offset);
      break;
    }
    auto decoded = decode_record(bytes);
    if (decoded.status != DecodeStatus::complete || !decoded.record.has_value()) {
      throw StorageError(StorageErrorCode::corruption,
                         "WAL corruption at offset " + std::to_string(offset) +
                             ": " + decoded.error);
    }
    if (decoded.record->lsn != recovered_lsn + 1U) {
      throw StorageError(StorageErrorCode::corruption,
                         "non-contiguous WAL LSN at offset " +
                             std::to_string(offset));
    }
    recovered_lsn = decoded.record->lsn;
    callback(*decoded.record);
    offset += decoded.consumed;
  }

  if (::lseek(descriptor_, 0, SEEK_END) < 0) {
    throw_io("seek WAL append position");
  }
  last_lsn_ = recovered_lsn;
  durable_lsn_ = recovered_lsn;
}

void Wal::close() noexcept {
  if (descriptor_ >= 0) {
    const auto descriptor = std::exchange(descriptor_, -1);
    static_cast<void>(::close(descriptor));
  }
}

std::uint64_t Wal::last_lsn() const noexcept { return last_lsn_; }

std::uint64_t Wal::durable_lsn() const noexcept { return durable_lsn_; }

}  // namespace forgekv::storage
