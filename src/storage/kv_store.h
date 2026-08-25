#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>

namespace forgekv::storage {

enum class DurabilityMode {
  async,
  sync,
  group_commit,
};

struct StorageHooks {
  std::function<void(std::uint64_t)> before_append;
  std::function<void(std::uint64_t)> after_append;
  std::function<void(std::uint64_t)> before_sync;
  std::function<void(std::uint64_t)> after_sync;
};

struct StorageOptions {
  std::filesystem::path wal_path;
  DurabilityMode durability{DurabilityMode::sync};
  std::size_t max_batch_entries{64U};
  std::size_t max_batch_bytes{4U * 1024U * 1024U};
  std::chrono::microseconds max_batch_wait{1000};
  std::size_t max_pending_entries{4096U};
  std::size_t max_pending_bytes{64U * 1024U * 1024U};
  std::function<void(std::size_t, std::size_t, std::uint64_t)> flush_observer;
  StorageHooks hooks;
};

struct DeleteResult {
  std::uint64_t lsn{0U};
  bool existed{false};
};

class KvStore final {
 public:
  [[nodiscard]] static std::unique_ptr<KvStore> open(StorageOptions options);

  KvStore(const KvStore&) = delete;
  KvStore& operator=(const KvStore&) = delete;
  ~KvStore();

  [[nodiscard]] std::uint64_t put(std::string key, std::string value);
  [[nodiscard]] std::optional<std::string> get(const std::string& key) const;
  [[nodiscard]] DeleteResult erase(std::string key);
  void close();

  [[nodiscard]] std::uint64_t last_lsn() const noexcept;
  [[nodiscard]] std::size_t size() const;

 private:
  class Impl;
  explicit KvStore(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

}  // namespace forgekv::storage
