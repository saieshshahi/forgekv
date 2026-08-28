#include "raft/raft_storage.h"
#include "raft/snapshot_store.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <string>
#include <vector>

#include <unistd.h>

namespace forgekv::raft {
namespace {

class TestDirectory final {
 public:
  TestDirectory() {
    static std::atomic<std::uint64_t> sequence{0};
    path_ = std::filesystem::temp_directory_path() /
            ("forgekv-raft-storage-" + std::to_string(::getpid()) + "-" +
             std::to_string(sequence.fetch_add(1)));
    std::filesystem::create_directories(path_);
  }
  ~TestDirectory() { std::filesystem::remove_all(path_); }
  [[nodiscard]] const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

LogEntry entry(const LogIndex index, const Term term,
               const unsigned char payload) {
  return LogEntry{
      .index = index,
      .term = term,
      .kind = EntryKind::command,
      .command = {static_cast<std::byte>(payload)},
  };
}

void persist(RaftStorage& storage, const PersistHardState& update) {
  storage.prepare(update);
  EXPECT_TRUE(storage.has_pending_update());
  storage.sync();
  EXPECT_FALSE(storage.has_pending_update());
}

void persist(RaftStorage& storage, const PersistLog& update) {
  storage.prepare(update);
  EXPECT_TRUE(storage.has_pending_update());
  storage.sync();
  EXPECT_FALSE(storage.has_pending_update());
}

void flip_byte(const std::filesystem::path& path, const std::uint64_t offset) {
  std::fstream stream(path, std::ios::binary | std::ios::in | std::ios::out);
  ASSERT_TRUE(stream.is_open());
  stream.seekg(static_cast<std::streamoff>(offset));
  char value = 0;
  stream.get(value);
  ASSERT_TRUE(stream);
  stream.seekp(static_cast<std::streamoff>(offset));
  stream.put(static_cast<char>(static_cast<unsigned char>(value) ^ 0x01U));
  stream.flush();
}

TEST(RaftStorageTest, EmptyDirectoryRecoversTermZeroAndCreatesJournal) {
  TestDirectory directory;
  auto storage = RaftStorage::open(directory.path(), 99, 7);

  EXPECT_EQ(storage.state().current_term, 0U);
  EXPECT_FALSE(storage.state().voted_for.has_value());
  EXPECT_TRUE(storage.state().log.empty());
  EXPECT_TRUE(std::filesystem::exists(directory.path() / "raft-log.wal"));
}

TEST(RaftStorageTest, RejectsConcurrentOwnershipOfOneNodeDirectory) {
  TestDirectory directory;
  auto owner = RaftStorage::open(directory.path(), 99, 7);
  EXPECT_THROW(static_cast<void>(RaftStorage::open(directory.path(), 99, 7)),
               RaftStorageError);
  owner.close();
  EXPECT_NO_THROW(
      static_cast<void>(RaftStorage::open(directory.path(), 99, 7)));
}

TEST(RaftStorageTest, LegacyVersionOneIdentityRequiresExplicitMigration) {
  TestDirectory directory;
  {
    auto storage = RaftStorage::open(directory.path(), 99, 7);
    storage.close();
  }
  std::filesystem::resize_file(directory.path() / "IDENTITY", 32U);
  try {
    static_cast<void>(RaftStorage::open(directory.path(), 99, 7));
    FAIL() << "legacy identity unexpectedly opened";
  } catch (const RaftStorageError& error) {
    EXPECT_NE(std::string(error.what()).find("version 1"), std::string::npos);
  }
}

TEST(RaftStorageTest, HardStateAlternatesChecksummedGenerations) {
  TestDirectory directory;
  {
    auto storage = RaftStorage::open(directory.path(), 99, 1);
    persist(storage, PersistHardState{.term = 1, .voted_for = 2});
    persist(storage, PersistHardState{.term = 2, .voted_for = 3});
  }

  auto recovered = RaftStorage::open(directory.path(), 99, 1);
  EXPECT_EQ(recovered.state().current_term, 2U);
  EXPECT_EQ(recovered.state().voted_for, 3U);
  EXPECT_TRUE(std::filesystem::exists(directory.path() / "hard-state.A"));
  EXPECT_TRUE(std::filesystem::exists(directory.path() / "hard-state.B"));
}

TEST(RaftStorageTest, AnyCorruptHardStateGenerationFailsClosed) {
  TestDirectory directory;
  {
    auto storage = RaftStorage::open(directory.path(), 99, 1);
    persist(storage, PersistHardState{.term = 1, .voted_for = 2});
    persist(storage, PersistHardState{.term = 2, .voted_for = 3});
  }
  flip_byte(directory.path() / "hard-state.B", 16);

  EXPECT_THROW(static_cast<void>(RaftStorage::open(directory.path(), 99, 1)),
               RaftStorageError);
}

TEST(RaftStorageTest, CorruptAcknowledgedVoteCannotBeForgotten) {
  TestDirectory directory;
  {
    auto storage = RaftStorage::open(directory.path(), 99, 1);
    persist(storage, PersistHardState{.term = 1, .voted_for = 2});
  }
  flip_byte(directory.path() / "hard-state.A", 48);

  try {
    static_cast<void>(RaftStorage::open(directory.path(), 99, 1));
    FAIL() << "corrupt acknowledged vote was silently forgotten";
  } catch (const RaftStorageError& error) {
    EXPECT_EQ(error.code(), RaftStorageErrorCode::corruption);
  }
}

TEST(RaftStorageTest, IgnoresUnpublishedHardStateTemporaryFile) {
  TestDirectory directory;
  {
    auto storage = RaftStorage::open(directory.path(), 99, 1);
    persist(storage, PersistHardState{.term = 2, .voted_for = 3});
  }
  {
    std::ofstream temporary(directory.path() / ".hard-state.A.tmp.stale",
                            std::ios::binary);
    temporary << "torn unpublished generation";
  }
  const auto recognized_temporary =
      directory.path() / ".hard-state.A.tmp.123.0";
  {
    std::ofstream temporary(recognized_temporary, std::ios::binary);
    temporary << "torn unpublished generation";
  }

  auto recovered = RaftStorage::open(directory.path(), 99, 1);
  EXPECT_EQ(recovered.state().current_term, 2U);
  EXPECT_EQ(recovered.state().voted_for, 3U);
  EXPECT_FALSE(std::filesystem::exists(recognized_temporary));
  EXPECT_TRUE(std::filesystem::exists(
      directory.path() / ".hard-state.A.tmp.stale"));
}

TEST(RaftStorageTest, MissingPublishedHardStateGenerationFailsClosed) {
  TestDirectory directory;
  {
    auto storage = RaftStorage::open(directory.path(), 99, 1);
    persist(storage, PersistHardState{.term = 1, .voted_for = 2});
  }
  ASSERT_TRUE(std::filesystem::remove(directory.path() / "hard-state.A"));

  EXPECT_THROW(static_cast<void>(RaftStorage::open(directory.path(), 99, 1)),
               RaftStorageError);
}

TEST(RaftStorageTest, MalformedBootstrapArtifactsFailClosed) {
  for (const bool corrupt_hard_state : {false, true}) {
    TestDirectory directory;
    {
      auto storage = RaftStorage::open(directory.path(), 99, 1);
    }
    const auto path = directory.path() /
                      (corrupt_hard_state ? "hard-state.A" : "raft-log.wal");
    std::ofstream malformed(path, std::ios::binary | std::ios::trunc);
    malformed << "short";
    malformed.close();

    EXPECT_THROW(static_cast<void>(RaftStorage::open(directory.path(), 99, 1)),
                 RaftStorageError)
        << path;
  }
}

TEST(RaftStorageTest, InitializedStoreNeverRecreatesMissingDurableArtifacts) {
  for (const int deletion_case : {0, 1, 2}) {
    TestDirectory directory;
    {
      auto storage = RaftStorage::open(directory.path(), 99, 1);
      persist(storage, PersistHardState{.term = 1, .voted_for = 2});
      persist(storage,
              PersistLog{.from_index = 1,
                         .entries = {entry(1, 1, 0x77)}});
    }
    if (deletion_case == 0) {
      ASSERT_TRUE(
          std::filesystem::remove(directory.path() / "hard-state.A"));
      ASSERT_TRUE(
          std::filesystem::remove(directory.path() / "hard-state.B"));
    } else if (deletion_case == 1) {
      ASSERT_TRUE(
          std::filesystem::remove(directory.path() / "raft-log.wal"));
    } else {
      ASSERT_TRUE(std::filesystem::remove(directory.path() / "IDENTITY"));
    }

    EXPECT_THROW(static_cast<void>(RaftStorage::open(directory.path(), 99, 1)),
                 RaftStorageError)
        << deletion_case;
  }
}

TEST(RaftStorageTest, RejectsDoubleVoteAndTermRegression) {
  TestDirectory directory;
  auto storage = RaftStorage::open(directory.path(), 99, 1);
  persist(storage, PersistHardState{.term = 4, .voted_for = 2});

  EXPECT_THROW(storage.prepare(PersistHardState{.term = 4, .voted_for = 3}),
               RaftStorageError);
  EXPECT_THROW(storage.prepare(PersistHardState{.term = 3, .voted_for = 2}),
               RaftStorageError);
  EXPECT_FALSE(storage.has_pending_update());
}

TEST(RaftStorageTest, ReplaysGapFreeLogAndConflictingSuffixReplacement) {
  TestDirectory directory;
  {
    auto storage = RaftStorage::open(directory.path(), 99, 1);
    persist(storage, PersistHardState{.term = 3, .voted_for = std::nullopt});
    persist(storage, PersistLog{
                         .from_index = 1,
                         .entries = {entry(1, 1, 0x11), entry(2, 2, 0x22)},
                     });
    persist(storage, PersistLog{
                         .from_index = 2,
                         .entries = {entry(2, 3, 0x33)},
                     });
  }

  auto recovered = RaftStorage::open(directory.path(), 99, 1);
  EXPECT_EQ(recovered.state().log,
            (std::vector<LogEntry>{entry(1, 1, 0x11), entry(2, 3, 0x33)}));
}

TEST(RaftStorageTest, SnapshotCompactionPersistsBoundaryAndRetainedSuffix) {
  TestDirectory directory;
  const StateMachineSnapshot snapshot{
      .last_included_index = 2,
      .last_included_term = 2,
      .state_machine = {std::byte{0xCA}, std::byte{0xFE}},
  };
  {
    auto storage = RaftStorage::open(directory.path(), 99, 1);
    persist(storage, PersistHardState{.term = 3, .voted_for = std::nullopt});
    persist(storage,
            PersistLog{.from_index = 1,
                       .entries = {entry(1, 1, 0x11), entry(2, 2, 0x22),
                                   entry(3, 3, 0x33)}});
    storage.install_snapshot(snapshot);
    EXPECT_EQ(storage.state().snapshot, snapshot);
    EXPECT_EQ(storage.state().log,
              (std::vector<LogEntry>{entry(3, 3, 0x33)}));
    persist(storage,
            PersistLog{.from_index = 4, .entries = {entry(4, 3, 0x44)}});
  }

  auto recovered = RaftStorage::open(directory.path(), 99, 1);
  EXPECT_EQ(recovered.state().snapshot, snapshot);
  EXPECT_EQ(recovered.state().log,
            (std::vector<LogEntry>{entry(3, 3, 0x33),
                                   entry(4, 3, 0x44)}));
}

TEST(RaftStorageTest, RecoveryAcceptsPublishedSnapshotWithOldFullJournal) {
  TestDirectory directory;
  const StateMachineSnapshot snapshot{
      .last_included_index = 2,
      .last_included_term = 2,
      .state_machine = {std::byte{0x5A}},
  };
  bool crash_after_snapshot_rename = false;
  {
    auto storage = RaftStorage::open(
        directory.path(), 99, 1,
        [&crash_after_snapshot_rename](const RaftStorageSyncPoint point) {
          if (crash_after_snapshot_rename &&
              point == RaftStorageSyncPoint::after_rename) {
            throw std::runtime_error("simulated crash after snapshot rename");
          }
        });
    persist(storage, PersistHardState{.term = 3, .voted_for = std::nullopt});
    persist(storage,
            PersistLog{.from_index = 1,
                       .entries = {entry(1, 1, 0x11), entry(2, 2, 0x22),
                                   entry(3, 3, 0x33)}});
    crash_after_snapshot_rename = true;
    EXPECT_THROW(storage.install_snapshot(snapshot), std::runtime_error);
  }

  auto recovered = RaftStorage::open(directory.path(), 99, 1);
  EXPECT_EQ(recovered.state().snapshot, snapshot);
  EXPECT_EQ(recovered.state().log,
            (std::vector<LogEntry>{entry(3, 3, 0x33)}));
}

TEST(RaftStorageTest,
     RecoveryValidatesFinalOverwrittenBoundaryBeforeUsingPublishedSnapshot) {
  TestDirectory directory;
  const StateMachineSnapshot snapshot{
      .last_included_index = 2,
      .last_included_term = 2,
      .state_machine = {std::byte{0x5B}},
  };
  {
    auto storage = RaftStorage::open(directory.path(), 99, 1);
    persist(storage, PersistHardState{.term = 3, .voted_for = std::nullopt});
    persist(storage,
            PersistLog{.from_index = 1,
                       .entries = {entry(1, 1, 0x11), entry(2, 1, 0x21),
                                   entry(3, 1, 0x31)}});
    persist(storage,
            PersistLog{.from_index = 2,
                       .entries = {entry(2, 2, 0x22), entry(3, 3, 0x33)}});
    SnapshotStore::write_atomic(directory.path(), 99, 1, 0, snapshot);
  }

  auto recovered = RaftStorage::open(directory.path(), 99, 1);
  EXPECT_EQ(recovered.state().snapshot, snapshot);
  EXPECT_EQ(recovered.state().log,
            (std::vector<LogEntry>{entry(3, 3, 0x33)}));
}

TEST(RaftStorageTest,
     RecoveryAcceptsNewSnapshotWithOlderCompactedJournal) {
  TestDirectory directory;
  const StateMachineSnapshot older{
      .last_included_index = 2,
      .last_included_term = 1,
      .state_machine = {std::byte{0xA2}},
  };
  const StateMachineSnapshot newer{
      .last_included_index = 4,
      .last_included_term = 3,
      .state_machine = {std::byte{0xA4}},
  };
  {
    auto storage = RaftStorage::open(directory.path(), 99, 1);
    persist(storage, PersistHardState{.term = 3, .voted_for = std::nullopt});
    persist(storage,
            PersistLog{.from_index = 1,
                       .entries = {entry(1, 1, 0x11), entry(2, 1, 0x22),
                                   entry(3, 2, 0x33), entry(4, 3, 0x44),
                                   entry(5, 3, 0x55)}});
    storage.install_snapshot(older);
    SnapshotStore::write_atomic(directory.path(), 99, 1, 0, newer);
  }

  auto recovered = RaftStorage::open(directory.path(), 99, 1);
  EXPECT_EQ(recovered.state().snapshot, newer);
  EXPECT_EQ(recovered.state().log,
            (std::vector<LogEntry>{entry(5, 3, 0x55)}));
}

TEST(RaftStorageTest, RejectsSnapshotBoundaryWithoutRepresentableSuccessor) {
  TestDirectory directory;
  auto storage = RaftStorage::open(directory.path(), 99, 1);
  persist(storage, PersistHardState{.term = 1, .voted_for = std::nullopt});
  EXPECT_THROW(
      storage.install_snapshot(StateMachineSnapshot{
          .last_included_index = std::numeric_limits<LogIndex>::max(),
          .last_included_term = 1,
          .state_machine = {std::byte{0xFF}},
      }),
      RaftStorageError);
}

TEST(RaftStorageTest, CompactedJournalFailsClosedWhenRequiredSnapshotIsMissing) {
  TestDirectory directory;
  {
    auto storage = RaftStorage::open(directory.path(), 99, 1);
    persist(storage, PersistHardState{.term = 1, .voted_for = std::nullopt});
    persist(storage,
            PersistLog{.from_index = 1, .entries = {entry(1, 1, 0x11)}});
    storage.install_snapshot(StateMachineSnapshot{
        .last_included_index = 1,
        .last_included_term = 1,
        .state_machine = {std::byte{0xA1}},
    });
  }
  std::filesystem::remove(SnapshotStore::published_path(directory.path()));
  EXPECT_THROW(static_cast<void>(RaftStorage::open(directory.path(), 99, 1)),
               RaftStorageError);
}

TEST(RaftStorageTest, CompactionSplitsLargeSuffixIntoRecoverableRecords) {
  TestDirectory directory;
  constexpr std::size_t total_entries = 4'102;
  {
    auto storage = RaftStorage::open(directory.path(), 99, 1);
    persist(storage, PersistHardState{.term = 2, .voted_for = std::nullopt});
    std::vector<LogEntry> first;
    first.reserve(kMaxRaftLogEntriesPerRecord);
    for (std::size_t offset = 0; offset < kMaxRaftLogEntriesPerRecord;
         ++offset) {
      first.push_back(entry(offset + 1, offset == 0 ? 1 : 2, 0x22));
    }
    persist(storage, PersistLog{.from_index = 1, .entries = std::move(first)});
    std::vector<LogEntry> second;
    for (std::size_t index = kMaxRaftLogEntriesPerRecord + 1;
         index <= total_entries; ++index) {
      second.push_back(entry(index, 2, 0x23));
    }
    persist(storage,
            PersistLog{.from_index = kMaxRaftLogEntriesPerRecord + 1,
                       .entries = std::move(second)});
    storage.install_snapshot(StateMachineSnapshot{
        .last_included_index = 1,
        .last_included_term = 1,
        .state_machine = {std::byte{0xB1}},
    });
  }
  auto recovered = RaftStorage::open(directory.path(), 99, 1);
  EXPECT_EQ(recovered.state().log.size(), total_entries - 1);
  EXPECT_EQ(recovered.state().log.front().index, 2U);
  EXPECT_EQ(recovered.state().log.back().index, total_entries);
}

TEST(RaftStorageTest, CompactedJournalCrashPointsRecoverTwice) {
  for (const auto crash_point : {RaftStorageSyncPoint::after_write,
                                 RaftStorageSyncPoint::after_file_sync,
                                 RaftStorageSyncPoint::after_rename,
                                 RaftStorageSyncPoint::after_directory_sync}) {
    TestDirectory directory;
    bool armed = false;
    const StateMachineSnapshot snapshot{
        .last_included_index = 2,
        .last_included_term = 2,
        .state_machine = {std::byte{0xC2}},
    };
    {
      auto storage = RaftStorage::open(
          directory.path(), 99, 1,
          [&armed, crash_point](const RaftStorageSyncPoint current) {
            if (armed && current == crash_point) {
              throw std::runtime_error("simulated compacted-journal crash");
            }
          });
      persist(storage,
              PersistHardState{.term = 3, .voted_for = std::nullopt});
      persist(storage,
              PersistLog{.from_index = 1,
                         .entries = {entry(1, 1, 0x11), entry(2, 2, 0x22),
                                     entry(3, 3, 0x33)}});
      SnapshotStore::write_atomic(directory.path(), 99, 1, 0, snapshot);
      armed = true;
      EXPECT_THROW(storage.install_snapshot(snapshot, true),
                   std::runtime_error);
    }
    for (int restart = 0; restart < 2; ++restart) {
      auto recovered = RaftStorage::open(directory.path(), 99, 1);
      EXPECT_EQ(recovered.state().snapshot, snapshot);
      EXPECT_EQ(recovered.state().log,
                (std::vector<LogEntry>{entry(3, 3, 0x33)}));
      recovered.close();
    }
  }
}

TEST(RaftStorageTest, TruncatesEveryIncompleteFinalJournalRecordPrefix) {
  TestDirectory directory;
  std::uintmax_t valid_size = 0;
  std::uintmax_t full_size = 0;
  {
    auto storage = RaftStorage::open(directory.path(), 99, 1);
    persist(storage, PersistHardState{.term = 1, .voted_for = std::nullopt});
    persist(storage,
            PersistLog{.from_index = 1, .entries = {entry(1, 1, 0x41)}});
    valid_size =
        std::filesystem::file_size(directory.path() / "raft-log.wal");
    persist(storage,
            PersistLog{.from_index = 2, .entries = {entry(2, 1, 0x42)}});
    full_size =
        std::filesystem::file_size(directory.path() / "raft-log.wal");
  }

  std::vector<char> complete(static_cast<std::size_t>(full_size));
  {
    std::ifstream stream(directory.path() / "raft-log.wal", std::ios::binary);
    stream.read(complete.data(), static_cast<std::streamsize>(complete.size()));
    ASSERT_TRUE(stream);
  }

  for (auto prefix = valid_size; prefix < full_size; ++prefix) {
    {
      std::ofstream stream(directory.path() / "raft-log.wal",
                           std::ios::binary | std::ios::trunc);
      stream.write(complete.data(), static_cast<std::streamsize>(prefix));
      ASSERT_TRUE(stream) << prefix;
    }
    auto recovered = RaftStorage::open(directory.path(), 99, 1);
    EXPECT_EQ(recovered.state().log,
              (std::vector<LogEntry>{entry(1, 1, 0x41)}))
        << prefix;
    EXPECT_EQ(std::filesystem::file_size(directory.path() / "raft-log.wal"),
              valid_size)
        << prefix;
    recovered.close();
  }
}

TEST(RaftStorageTest, FailsClosedOnCompleteJournalChecksumCorruption) {
  TestDirectory directory;
  {
    auto storage = RaftStorage::open(directory.path(), 99, 1);
    persist(storage, PersistHardState{.term = 1, .voted_for = std::nullopt});
    persist(storage,
            PersistLog{.from_index = 1, .entries = {entry(1, 1, 0x52)}});
  }
  flip_byte(directory.path() / "raft-log.wal", 40);

  try {
    static_cast<void>(RaftStorage::open(directory.path(), 99, 1));
    FAIL() << "corrupt Raft journal was accepted";
  } catch (const RaftStorageError& error) {
    EXPECT_EQ(error.code(), RaftStorageErrorCode::corruption);
  }
}

TEST(RaftStorageTest, FailsClosedWhenAnyRecordLengthByteChanges) {
  for (const bool interior : {false, true}) {
    for (std::uint64_t byte = 0; byte < 4; ++byte) {
      TestDirectory directory;
      std::uintmax_t second_record_offset = 0;
      {
        auto storage = RaftStorage::open(directory.path(), 99, 1);
        persist(storage,
                PersistHardState{.term = 1, .voted_for = std::nullopt});
        persist(storage,
                PersistLog{.from_index = 1,
                           .entries = {entry(1, 1, 0x61)}});
        second_record_offset =
            std::filesystem::file_size(directory.path() / "raft-log.wal");
        persist(storage,
                PersistLog{.from_index = 2,
                           .entries = {entry(2, 1, 0x62)}});
      }

      constexpr std::uint64_t log_header_size = 52;
      const auto record_offset =
          interior ? log_header_size : second_record_offset;
      flip_byte(directory.path() / "raft-log.wal",
                record_offset + 8 + byte);
      try {
        static_cast<void>(RaftStorage::open(directory.path(), 99, 1));
        FAIL() << "mutated record length was accepted: interior=" << interior
               << " byte=" << byte;
      } catch (const RaftStorageError& error) {
        EXPECT_EQ(error.code(), RaftStorageErrorCode::corruption);
      }
    }
  }
}

TEST(RaftStorageTest, RejectsWrongNodeIdentityAndInvalidLogUpdates) {
  TestDirectory directory;
  auto storage = RaftStorage::open(directory.path(), 99, 1);
  persist(storage, PersistHardState{.term = 2, .voted_for = std::nullopt});
  persist(storage,
          PersistLog{.from_index = 1, .entries = {entry(1, 1, 0x10)}});
  storage.close();

  EXPECT_THROW(static_cast<void>(RaftStorage::open(directory.path(), 99, 2)),
               RaftStorageError);
  EXPECT_THROW(static_cast<void>(RaftStorage::open(directory.path(), 100, 1)),
               RaftStorageError);

  auto reopened = RaftStorage::open(directory.path(), 99, 1);
  EXPECT_THROW(reopened.prepare(
                   PersistLog{.from_index = 3,
                              .entries = {entry(3, 2, 0x30)}}),
               RaftStorageError);
  EXPECT_THROW(reopened.prepare(
                   PersistLog{.from_index = 2,
                              .entries = {entry(2, 3, 0x30)}}),
               RaftStorageError);
}

}  // namespace
}  // namespace forgekv::raft
