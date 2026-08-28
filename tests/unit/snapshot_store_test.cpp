#include "raft/snapshot_store.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include <unistd.h>

namespace forgekv::raft {
namespace {

class SnapshotDirectory final {
 public:
  SnapshotDirectory() {
    static std::atomic<std::uint64_t> sequence{0};
    path_ = std::filesystem::temp_directory_path() /
            ("forgekv-snapshot-test-" + std::to_string(::getpid()) + "-" +
             std::to_string(sequence.fetch_add(1U)));
    std::filesystem::create_directories(path_);
  }
  ~SnapshotDirectory() { std::filesystem::remove_all(path_); }
  [[nodiscard]] const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

StateMachineSnapshot snapshot(const LogIndex index, const Term term,
                              const std::string& value) {
  const auto* begin = reinterpret_cast<const std::byte*>(value.data());
  return StateMachineSnapshot{.last_included_index = index,
                              .last_included_term = term,
                              .state_machine = {begin, begin + value.size()}};
}

std::vector<char> bytes(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  return {std::istreambuf_iterator<char>(input),
          std::istreambuf_iterator<char>()};
}

TEST(SnapshotStoreTest, EmptyDirectoryHasNoPublishedSnapshot) {
  SnapshotDirectory directory;
  EXPECT_EQ(SnapshotStore::load(directory.path(), 7, 1, 99), std::nullopt);
}

TEST(SnapshotStoreTest, RoundTripsDeterministicVersionedChecksummedImage) {
  SnapshotDirectory left;
  SnapshotDirectory right;
  const auto expected = snapshot(41, 6, std::string("a\0b", 3));
  SnapshotStore::write_atomic(left.path(), 7, 1, 99, expected);
  SnapshotStore::write_atomic(right.path(), 7, 1, 99, expected);

  EXPECT_EQ(SnapshotStore::load(left.path(), 7, 1, 99), expected);
  EXPECT_EQ(bytes(SnapshotStore::published_path(left.path())),
            bytes(SnapshotStore::published_path(right.path())));
  EXPECT_FALSE(std::filesystem::exists(
      SnapshotStore::temporary_path(left.path())));
}

TEST(SnapshotStoreTest, RejectsWrongIdentityAndCompleteCorruption) {
  SnapshotDirectory directory;
  SnapshotStore::write_atomic(directory.path(), 7, 1, 99,
                              snapshot(41, 6, "payload"));
  EXPECT_THROW(
      static_cast<void>(SnapshotStore::load(directory.path(), 8, 1, 99)),
      RaftStorageError);
  EXPECT_THROW(
      static_cast<void>(SnapshotStore::load(directory.path(), 7, 2, 99)),
      RaftStorageError);
  EXPECT_THROW(
      static_cast<void>(SnapshotStore::load(directory.path(), 7, 1, 100)),
      RaftStorageError);

  auto image = bytes(SnapshotStore::published_path(directory.path()));
  ASSERT_GT(image.size(), 64U);
  image.back() = static_cast<char>(image.back() ^ 0x5A);
  std::ofstream output(SnapshotStore::published_path(directory.path()),
                       std::ios::binary | std::ios::trunc);
  output.write(image.data(), static_cast<std::streamsize>(image.size()));
  output.close();
  EXPECT_THROW(
      static_cast<void>(SnapshotStore::load(directory.path(), 7, 1, 99)),
      RaftStorageError);
}

TEST(SnapshotStoreTest, PublicationCrashPointsChooseOldOrNewValidImage) {
  for (const auto point : {SnapshotWritePoint::after_write,
                           SnapshotWritePoint::after_file_sync,
                           SnapshotWritePoint::after_rename,
                           SnapshotWritePoint::after_directory_sync}) {
    SnapshotDirectory directory;
    const auto old_snapshot = snapshot(10, 2, "old");
    const auto new_snapshot = snapshot(20, 3, "new");
    SnapshotStore::write_atomic(directory.path(), 7, 1, 99, old_snapshot);
    EXPECT_THROW(
        SnapshotStore::write_atomic(
            directory.path(), 7, 1, 99, new_snapshot,
            [point](const SnapshotWritePoint current) {
              if (current == point) {
                throw std::runtime_error("simulated crash");
              }
            }),
        std::runtime_error);
    const auto recovered = SnapshotStore::load(directory.path(), 7, 1, 99);
    ASSERT_TRUE(recovered.has_value());
    if (point == SnapshotWritePoint::after_write ||
        point == SnapshotWritePoint::after_file_sync) {
      EXPECT_EQ(*recovered, old_snapshot);
    } else {
      EXPECT_EQ(*recovered, new_snapshot);
    }
  }
}

TEST(SnapshotStoreTest, IgnoresUnpublishedTemporaryFile) {
  SnapshotDirectory directory;
  std::ofstream temporary(SnapshotStore::temporary_path(directory.path()),
                          std::ios::binary | std::ios::trunc);
  temporary << "incomplete";
  temporary.close();
  EXPECT_EQ(SnapshotStore::load(directory.path(), 7, 1, 99), std::nullopt);
}

TEST(SnapshotStoreTest, DelayedOlderPublicationCannotReplaceNewerSnapshot) {
  SnapshotDirectory directory;
  const auto older = snapshot(10, 2, "older");
  const auto newer = snapshot(20, 3, "newer");
  SnapshotStore::write_atomic(directory.path(), 7, 1, 99, newer);
  SnapshotStore::write_atomic(directory.path(), 7, 1, 99, older);
  EXPECT_EQ(SnapshotStore::load(directory.path(), 7, 1, 99), newer);
}

TEST(SnapshotStoreTest, RejectsBoundaryWithoutRepresentableSuccessor) {
  SnapshotDirectory directory;
  EXPECT_THROW(
      SnapshotStore::write_atomic(
          directory.path(), 7, 1, 99,
          snapshot(std::numeric_limits<LogIndex>::max(), 1, "terminal")),
      RaftStorageError);
}

}  // namespace
}  // namespace forgekv::raft
