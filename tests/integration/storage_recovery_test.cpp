#include "storage/kv_store.h"

#include <atomic>
#include <filesystem>
#include <string>

#include <gtest/gtest.h>
#include <unistd.h>

namespace forgekv::storage {
namespace {

StorageOptions recovery_options(const std::filesystem::path& path,
                                const DurabilityMode mode) {
  StorageOptions options;
  options.wal_path = path;
  options.durability = mode;
  return options;
}

TEST(StorageRecoveryTest, ReconstructsExactStateInEveryDurabilityMode) {
  static std::atomic<std::uint64_t> sequence{0U};
  for (const auto mode : {DurabilityMode::async, DurabilityMode::sync,
                          DurabilityMode::group_commit}) {
    const auto root = std::filesystem::temp_directory_path() /
                      ("forgekv-restart-" + std::to_string(::getpid()) + "-" +
                       std::to_string(sequence.fetch_add(1U)));
    std::filesystem::create_directories(root);
    const auto path = root / "store.wal";
    {
      auto store = KvStore::open(recovery_options(path, mode));
      static_cast<void>(store->put("keep", std::string{"a\0b", 3}));
      static_cast<void>(store->put("drop", "old"));
      static_cast<void>(store->erase("drop"));
      static_cast<void>(store->put("keep", "new"));
    }
    {
      auto recovered = KvStore::open(recovery_options(path, mode));
      EXPECT_EQ(recovered->get("keep"), "new");
      EXPECT_FALSE(recovered->get("drop").has_value());
      EXPECT_EQ(recovered->last_lsn(), 4U);
    }
    std::filesystem::remove_all(root);
  }
}

}  // namespace
}  // namespace forgekv::storage
