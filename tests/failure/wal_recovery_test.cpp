#include "storage/wal.h"

#include <atomic>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <unistd.h>

namespace forgekv::storage {
namespace {

class RecoveryDirectory final {
 public:
  RecoveryDirectory() {
    static std::atomic<std::uint64_t> sequence{0U};
    root_ = std::filesystem::temp_directory_path() /
            ("forgekv-recovery-" + std::to_string(::getpid()) + "-" +
             std::to_string(sequence.fetch_add(1U)));
    std::filesystem::create_directories(root_);
  }
  ~RecoveryDirectory() { std::filesystem::remove_all(root_); }
  [[nodiscard]] std::filesystem::path file() const { return root_ / "store.wal"; }

 private:
  std::filesystem::path root_;
};

void write_bytes(const std::filesystem::path& path,
                 const std::vector<std::byte>& bytes) {
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  output.write(reinterpret_cast<const char*>(bytes.data()),
               static_cast<std::streamsize>(bytes.size()));
  ASSERT_TRUE(output.good());
}

TEST(WalRecoveryTest, TruncatesEveryIncompleteFinalRecordPrefix) {
  const auto first = encode_record({1U, WalOperation::put, "stable", "value"});
  const auto second = encode_record({2U, WalOperation::put, "torn", "payload"});

  for (std::size_t prefix = 0U; prefix < second.size(); ++prefix) {
    RecoveryDirectory directory;
    auto bytes = first;
    bytes.insert(bytes.end(), second.begin(),
                 second.begin() + static_cast<std::ptrdiff_t>(prefix));
    write_bytes(directory.file(), bytes);

    auto wal = Wal::open(directory.file());
    std::vector<WalRecord> recovered;
    wal.recover([&](const WalRecord& record) { recovered.push_back(record); });
    ASSERT_EQ(recovered.size(), 1U) << prefix;
    EXPECT_EQ(recovered.front().lsn, 1U) << prefix;
    EXPECT_EQ(std::filesystem::file_size(directory.file()), first.size()) << prefix;
  }
}

TEST(WalRecoveryTest, FailsClosedOnCompleteChecksumCorruption) {
  RecoveryDirectory directory;
  auto bytes = encode_record({1U, WalOperation::put, "key", "value"});
  bytes.back() ^= std::byte{0x80};
  write_bytes(directory.file(), bytes);
  try {
    static_cast<void>(Wal::open(directory.file()));
    FAIL() << "corruption should prevent opening";
  } catch (const StorageError& error) {
    EXPECT_EQ(error.code(), StorageErrorCode::corruption);
  }
}

TEST(WalRecoveryTest, FailsClosedOnACompleteDuplicateLsn) {
  RecoveryDirectory directory;
  auto bytes = encode_record({1U, WalOperation::put, "a", "one"});
  const auto duplicate = encode_record({1U, WalOperation::put, "b", "two"});
  bytes.insert(bytes.end(), duplicate.begin(), duplicate.end());
  write_bytes(directory.file(), bytes);
  EXPECT_THROW(static_cast<void>(Wal::open(directory.file())), StorageError);
}

}  // namespace
}  // namespace forgekv::storage
