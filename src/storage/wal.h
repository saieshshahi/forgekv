#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <stdexcept>

#include "storage/wal_record.h"

namespace forgekv::storage {

enum class StorageErrorCode {
  io,
  corruption,
  invalid_lsn,
  closed,
};

class StorageError : public std::runtime_error {
 public:
  StorageError(StorageErrorCode code, const std::string& message);
  [[nodiscard]] StorageErrorCode code() const noexcept;

 private:
  StorageErrorCode code_;
};

class Wal final {
 public:
  using ReplayCallback = std::function<void(const WalRecord&)>;

  [[nodiscard]] static Wal open(const std::filesystem::path& path);

  Wal(Wal&& other) noexcept;
  Wal& operator=(Wal&& other) noexcept;
  Wal(const Wal&) = delete;
  Wal& operator=(const Wal&) = delete;
  ~Wal();

  void append(const WalRecord& record);
  void sync();
  void recover(const ReplayCallback& callback);
  void close() noexcept;

  [[nodiscard]] std::uint64_t last_lsn() const noexcept;
  [[nodiscard]] std::uint64_t durable_lsn() const noexcept;

 private:
  Wal(int descriptor, std::filesystem::path path);

  int descriptor_{-1};
  std::filesystem::path path_;
  std::uint64_t last_lsn_{0U};
  std::uint64_t durable_lsn_{0U};
};

}  // namespace forgekv::storage
