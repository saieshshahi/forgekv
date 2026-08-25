#include "storage/kv_store.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <future>
#include <mutex>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <unistd.h>

#include "storage/wal_record.h"

namespace forgekv::storage {
namespace {

class StoreDirectory final {
 public:
  StoreDirectory() {
    static std::atomic<std::uint64_t> sequence{0U};
    root_ = std::filesystem::temp_directory_path() /
            ("forgekv-store-" + std::to_string(::getpid()) + "-" +
             std::to_string(sequence.fetch_add(1U)));
    std::filesystem::create_directories(root_);
  }
  ~StoreDirectory() { std::filesystem::remove_all(root_); }
  [[nodiscard]] std::filesystem::path wal() const { return root_ / "store.wal"; }

 private:
  std::filesystem::path root_;
};

StorageOptions store_options(const std::filesystem::path& path,
                             const DurabilityMode mode = DurabilityMode::sync) {
  StorageOptions options;
  options.wal_path = path;
  options.durability = mode;
  return options;
}

TEST(KvStoreTest, PutOverwriteGetAndDeleteAreOrdered) {
  StoreDirectory directory;
  auto store = KvStore::open(store_options(directory.wal()));
  EXPECT_FALSE(store->get("key").has_value());
  EXPECT_EQ(store->put("key", "one"), 1U);
  EXPECT_EQ(store->put("key", "two"), 2U);
  EXPECT_EQ(store->get("key"), "two");
  EXPECT_EQ(store->size(), 1U);
  EXPECT_TRUE(store->erase("key").existed);
  EXPECT_FALSE(store->erase("key").existed);
  EXPECT_FALSE(store->get("key").has_value());
  EXPECT_EQ(store->last_lsn(), 4U);
}

TEST(KvStoreTest, ValidatesKeyValueAndOptionsBounds) {
  StoreDirectory directory;
  auto store = KvStore::open(store_options(directory.wal()));
  EXPECT_THROW(static_cast<void>(store->put("", "value")), std::invalid_argument);
  EXPECT_THROW(static_cast<void>(store->get("")), std::invalid_argument);
  EXPECT_THROW(static_cast<void>(
                   store->erase(std::string(kMaxKeySize + 1U, 'k'))),
               std::invalid_argument);
  EXPECT_THROW(static_cast<void>(
                   store->put("key", std::string(kMaxValueSize + 1U, 'v'))),
               std::invalid_argument);

  auto invalid = store_options(directory.wal());
  invalid.max_batch_entries = 0U;
  EXPECT_THROW(static_cast<void>(KvStore::open(invalid)), std::invalid_argument);
}

TEST(KvStoreTest, SyncAndAsyncHaveDistinctFlushSemantics) {
  for (const auto mode : {DurabilityMode::sync, DurabilityMode::async}) {
    StoreDirectory directory;
    std::atomic<std::size_t> flushes{0U};
    auto options = store_options(directory.wal(), mode);
    options.flush_observer = [&](std::size_t, std::size_t, std::uint64_t) {
      flushes.fetch_add(1U);
    };
    auto store = KvStore::open(std::move(options));
    static_cast<void>(store->put("a", "1"));
    static_cast<void>(store->put("b", "2"));
    EXPECT_EQ(flushes.load(), mode == DurabilityMode::sync ? 2U : 0U);
  }
}

TEST(KvStoreTest, GroupCommitBatchesConcurrentWritersWithinBounds) {
  StoreDirectory directory;
  std::mutex observed_mutex;
  std::vector<std::size_t> batch_sizes;
  auto options = store_options(directory.wal(), DurabilityMode::group_commit);
  options.max_batch_entries = 4U;
  options.max_batch_bytes = 4096U;
  options.max_batch_wait = std::chrono::milliseconds(50);
  options.flush_observer = [&](const std::size_t entries, std::size_t,
                               std::uint64_t) {
    std::lock_guard lock(observed_mutex);
    batch_sizes.push_back(entries);
  };
  auto store = KvStore::open(std::move(options));

  std::vector<std::future<std::uint64_t>> writes;
  for (std::size_t index = 0U; index < 12U; ++index) {
    writes.push_back(std::async(std::launch::async, [&, index] {
      return store->put("key-" + std::to_string(index), "value");
    }));
  }
  for (auto& write : writes) {
    EXPECT_GE(write.get(), 1U);
  }

  ASSERT_FALSE(batch_sizes.empty());
  std::size_t total = 0U;
  bool saw_batch = false;
  for (const auto entries : batch_sizes) {
    total += entries;
    saw_batch = saw_batch || entries > 1U;
    EXPECT_LE(entries, 4U);
  }
  EXPECT_EQ(total, 12U);
  EXPECT_TRUE(saw_batch);
  EXPECT_EQ(store->last_lsn(), 12U);
}

TEST(KvStoreTest, GroupCommitRespectsTheBatchByteLimit) {
  StoreDirectory directory;
  std::mutex observed_mutex;
  std::vector<std::size_t> batch_sizes;
  StorageOptions options;
  options.wal_path = directory.wal();
  options.durability = DurabilityMode::group_commit;
  options.max_batch_entries = 8U;
  options.max_batch_bytes = 150U;
  options.max_batch_wait = std::chrono::milliseconds(20);
  options.flush_observer = [&](const std::size_t entries, std::size_t,
                               std::uint64_t) {
    std::lock_guard lock(observed_mutex);
    batch_sizes.push_back(entries);
  };
  auto store = KvStore::open(std::move(options));
  std::vector<std::future<std::uint64_t>> writes;
  for (std::size_t index = 0U; index < 6U; ++index) {
    writes.push_back(std::async(std::launch::async, [&, index] {
      return store->put("key-" + std::to_string(index), std::string(64U, 'v'));
    }));
  }
  for (auto& write : writes) {
    static_cast<void>(write.get());
  }
  ASSERT_EQ(batch_sizes.size(), 6U);
  EXPECT_TRUE(std::ranges::all_of(batch_sizes,
                                  [](const auto entries) { return entries == 1U; }));
}

TEST(KvStoreTest, ConcurrentReadsAndWritesAreSafe) {
  StoreDirectory directory;
  auto store = KvStore::open(
      store_options(directory.wal(), DurabilityMode::async));
  std::vector<std::future<void>> workers;
  for (std::size_t worker = 0U; worker < 4U; ++worker) {
    workers.push_back(std::async(std::launch::async, [&, worker] {
      for (std::size_t index = 0U; index < 50U; ++index) {
        const auto key = "key-" + std::to_string((worker + index) % 8U);
        static_cast<void>(store->put(key, std::to_string(index)));
        static_cast<void>(store->get(key));
      }
    }));
  }
  for (auto& worker : workers) {
    worker.get();
  }
  EXPECT_EQ(store->last_lsn(), 200U);
}

TEST(KvStoreTest, CloseIsIdempotentAndRejectsNewWork) {
  StoreDirectory directory;
  auto store = KvStore::open(store_options(directory.wal()));
  store->close();
  store->close();
  EXPECT_THROW(static_cast<void>(store->put("key", "value")),
               std::runtime_error);
}

}  // namespace
}  // namespace forgekv::storage
