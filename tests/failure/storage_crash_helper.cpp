#include "storage/kv_store.h"

#include <cerrno>
#include <csignal>
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <span>
#include <string>

#include <fcntl.h>
#include <unistd.h>

#include "storage/wal.h"
#include "storage/wal_record.h"

namespace {

[[noreturn]] void crash_now() {
  ::kill(::getpid(), SIGKILL);
  ::_exit(125);
}

void write_prefix_and_crash(const std::filesystem::path& path) {
  auto wal = forgekv::storage::Wal::open(path);
  wal.append({1U, forgekv::storage::WalOperation::put, "base", "durable"});
  wal.sync();

  const auto second = forgekv::storage::encode_record(
      {2U, forgekv::storage::WalOperation::put, "second", "value"});
  const auto descriptor = ::open(path.c_str(), O_WRONLY | O_APPEND | O_CLOEXEC);
  if (descriptor < 0) {
    ::_exit(124);
  }
  const auto prefix_size = second.size() / 2U;
  std::size_t written = 0U;
  while (written < prefix_size) {
    const auto result = ::write(descriptor, second.data() + written,
                                prefix_size - written);
    if (result > 0) {
      written += static_cast<std::size_t>(result);
    } else if (result < 0 && errno == EINTR) {
      continue;
    } else {
      ::_exit(123);
    }
  }
  static_cast<void>(::fdatasync(descriptor));
  crash_now();
}

}  // namespace

int main(const int argc, char** argv) {
  if (argc != 3) {
    return 2;
  }
  const std::filesystem::path path{argv[1]};
  const std::string point{argv[2]};
  if (point == "partial_append") {
    write_prefix_and_crash(path);
  }

  forgekv::storage::StorageOptions options;
  options.wal_path = path;
  options.durability = forgekv::storage::DurabilityMode::sync;
  options.hooks.before_append = [&](const std::uint64_t lsn) {
    if (lsn == 2U && point == "before_append") {
      crash_now();
    }
  };
  options.hooks.after_append = [&](const std::uint64_t lsn) {
    if (lsn == 2U && point == "after_append") {
      crash_now();
    }
  };
  options.hooks.before_sync = [&](const std::uint64_t lsn) {
    if (lsn == 2U && point == "before_sync") {
      crash_now();
    }
  };
  options.hooks.after_sync = [&](const std::uint64_t lsn) {
    if (lsn == 2U && point == "after_sync") {
      crash_now();
    }
  };

  auto store = forgekv::storage::KvStore::open(std::move(options));
  static_cast<void>(store->put("base", "durable"));
  static_cast<void>(store->put("second", "value"));
  return 3;
}
