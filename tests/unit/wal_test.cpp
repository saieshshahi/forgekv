#include "storage/wal.h"

#include <atomic>
#include <filesystem>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <unistd.h>

namespace forgekv::storage {
namespace {

class TemporaryDirectory final {
 public:
  TemporaryDirectory() {
    static std::atomic<std::uint64_t> sequence{0U};
    path_ = std::filesystem::temp_directory_path() /
            ("forgekv-wal-" + std::to_string(::getpid()) + "-" +
             std::to_string(sequence.fetch_add(1U)));
    std::filesystem::create_directories(path_);
  }
  ~TemporaryDirectory() { std::filesystem::remove_all(path_); }
  [[nodiscard]] std::filesystem::path file() const { return path_ / "store.wal"; }

 private:
  std::filesystem::path path_;
};

TEST(WalTest, OpensAndRecoversAnEmptyFile) {
  TemporaryDirectory directory;
  auto wal = Wal::open(directory.file());
  std::vector<WalRecord> records;
  wal.recover([&](const WalRecord& record) { records.push_back(record); });
  EXPECT_TRUE(records.empty());
  EXPECT_EQ(wal.last_lsn(), 0U);
  EXPECT_EQ(std::filesystem::file_size(directory.file()), 0U);
}

TEST(WalTest, AppendsSyncsAndReplaysInOrder) {
  TemporaryDirectory directory;
  {
    auto wal = Wal::open(directory.file());
    wal.append({1U, WalOperation::put, std::string{"a\0", 2},
                std::string{"v\0x", 3}});
    wal.append({2U, WalOperation::delete_key, std::string{"a\0", 2}, {}});
    EXPECT_EQ(wal.last_lsn(), 2U);
    EXPECT_EQ(wal.durable_lsn(), 0U);
    wal.sync();
    EXPECT_EQ(wal.durable_lsn(), 2U);
  }

  auto reopened = Wal::open(directory.file());
  std::vector<WalRecord> records;
  reopened.recover([&](const WalRecord& record) { records.push_back(record); });
  ASSERT_EQ(records.size(), 2U);
  EXPECT_EQ(records[0], (WalRecord{1U, WalOperation::put, std::string{"a\0", 2},
                                   std::string{"v\0x", 3}}));
  EXPECT_EQ(records[1].operation, WalOperation::delete_key);
  EXPECT_EQ(reopened.last_lsn(), 2U);
}

TEST(WalTest, RequiresGapFreeMonotonicLsns) {
  TemporaryDirectory directory;
  auto wal = Wal::open(directory.file());
  EXPECT_THROW(wal.append({2U, WalOperation::put, "k", "v"}), StorageError);
  wal.append({1U, WalOperation::put, "k", "v"});
  EXPECT_THROW(wal.append({1U, WalOperation::put, "k", "v2"}), StorageError);
  EXPECT_THROW(wal.append({3U, WalOperation::put, "k", "v3"}), StorageError);
}

TEST(WalTest, CloseIsIdempotentAndStopsWrites) {
  TemporaryDirectory directory;
  auto wal = Wal::open(directory.file());
  wal.close();
  wal.close();
  try {
    wal.append({1U, WalOperation::put, "k", "v"});
    FAIL() << "append should fail after close";
  } catch (const StorageError& error) {
    EXPECT_EQ(error.code(), StorageErrorCode::closed);
  }
}

}  // namespace
}  // namespace forgekv::storage
