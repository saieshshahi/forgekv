#include "storage/kv_store.h"

#include <atomic>
#include <filesystem>
#include <string>

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

namespace forgekv::storage {
namespace {

class CrashDirectory final {
 public:
  CrashDirectory() {
    static std::atomic<std::uint64_t> sequence{0U};
    root_ = std::filesystem::temp_directory_path() /
            ("forgekv-crash-" + std::to_string(::getpid()) + "-" +
             std::to_string(sequence.fetch_add(1U)));
    std::filesystem::create_directories(root_);
  }
  ~CrashDirectory() { std::filesystem::remove_all(root_); }
  [[nodiscard]] std::filesystem::path wal() const { return root_ / "store.wal"; }

 private:
  std::filesystem::path root_;
};

void run_crash_helper(const std::filesystem::path& path, const char* point) {
  const auto child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    ::execl(FORGEKV_CRASH_HELPER_PATH, FORGEKV_CRASH_HELPER_PATH,
            path.c_str(), point, nullptr);
    ::_exit(126);
  }
  int status = 0;
  ASSERT_EQ(::waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFSIGNALED(status));
  EXPECT_EQ(WTERMSIG(status), SIGKILL);
}

TEST(StorageCrashTest, RecoversOnlyCompleteRecordsAtEveryIoBoundary) {
  struct Case {
    const char* point;
    bool second_record_is_complete;
  };
  for (const auto& test_case : {
           Case{"before_append", false},
           Case{"partial_append", false},
           Case{"after_append", true},
           Case{"before_sync", true},
           Case{"after_sync", true},
       }) {
    CrashDirectory directory;
    run_crash_helper(directory.wal(), test_case.point);

    StorageOptions options;
    options.wal_path = directory.wal();
    options.durability = DurabilityMode::sync;
    auto recovered = KvStore::open(std::move(options));
    EXPECT_EQ(recovered->get("base"), "durable") << test_case.point;
    if (test_case.second_record_is_complete) {
      EXPECT_EQ(recovered->get("second"), "value") << test_case.point;
      EXPECT_EQ(recovered->last_lsn(), 2U) << test_case.point;
    } else {
      EXPECT_FALSE(recovered->get("second").has_value()) << test_case.point;
      EXPECT_EQ(recovered->last_lsn(), 1U) << test_case.point;
    }
  }
}

}  // namespace
}  // namespace forgekv::storage
