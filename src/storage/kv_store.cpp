#include "storage/kv_store.h"

#include <atomic>
#include <condition_variable>
#include <deque>
#include <exception>
#include <future>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "storage/wal.h"
#include "storage/wal_record.h"

namespace forgekv::storage {
namespace {

void validate_key(const std::string& key) {
  if (key.empty() || key.size() > kMaxKeySize) {
    throw std::invalid_argument("key length is outside the supported range");
  }
}

void validate_options(const StorageOptions& options) {
  if (options.wal_path.empty()) {
    throw std::invalid_argument("WAL path cannot be empty");
  }
  if (options.max_batch_entries == 0U || options.max_batch_bytes == 0U ||
      options.max_batch_wait.count() < 0 || options.max_pending_entries == 0U ||
      options.max_pending_bytes < kMaxWalRecordSize) {
    throw std::invalid_argument("storage queue and batch limits must be positive");
  }
}

struct MutationResult {
  std::uint64_t lsn{0U};
  bool existed{false};
};

struct PendingMutation {
  WalOperation operation{WalOperation::put};
  std::string key;
  std::string value;
  std::size_t bytes{0U};
  std::promise<MutationResult> completion;
};

}  // namespace

class KvStore::Impl final {
 public:
  Impl(StorageOptions options, Wal wal)
      : options_(std::move(options)), wal_(std::move(wal)) {
    wal_.recover([this](const WalRecord& record) { apply_recovered(record); });
    visible_lsn_.store(wal_.last_lsn(), std::memory_order_release);
    writer_ = std::thread([this] { writer_loop(); });
  }

  ~Impl() { close(); }

  std::uint64_t put(std::string key, std::string value) {
    validate_key(key);
    if (value.size() > kMaxValueSize) {
      throw std::invalid_argument("value exceeds the maximum size");
    }
    return submit(WalOperation::put, std::move(key), std::move(value)).lsn;
  }

  DeleteResult erase(std::string key) {
    validate_key(key);
    const auto result = submit(WalOperation::delete_key, std::move(key), {});
    return {result.lsn, result.existed};
  }

  std::optional<std::string> get(const std::string& key) const {
    validate_key(key);
    std::shared_lock lock(state_mutex_);
    const auto found = values_.find(key);
    if (found == values_.end()) {
      return std::nullopt;
    }
    return found->second;
  }

  std::size_t size() const {
    std::shared_lock lock(state_mutex_);
    return values_.size();
  }

  std::uint64_t last_lsn() const noexcept {
    return visible_lsn_.load(std::memory_order_acquire);
  }

  void close() {
    std::lock_guard close_lock(close_mutex_);
    {
      std::lock_guard lock(queue_mutex_);
      if (joined_) {
        return;
      }
      accepting_ = false;
      stopping_ = true;
    }
    work_available_.notify_all();
    capacity_available_.notify_all();
    if (writer_.joinable()) {
      writer_.join();
    }
    std::lock_guard lock(queue_mutex_);
    joined_ = true;
  }

 private:
  MutationResult submit(WalOperation operation, std::string key,
                        std::string value) {
    auto mutation = std::make_shared<PendingMutation>();
    mutation->operation = operation;
    mutation->key = std::move(key);
    mutation->value = std::move(value);
    mutation->bytes = kWalHeaderSize + mutation->key.size() + mutation->value.size();
    auto future = mutation->completion.get_future();

    {
      std::unique_lock lock(queue_mutex_);
      capacity_available_.wait(lock, [&] {
        return !accepting_ || failure_ != nullptr ||
               (pending_entries_ < options_.max_pending_entries &&
                pending_bytes_ + mutation->bytes <= options_.max_pending_bytes);
      });
      if (failure_ != nullptr) {
        std::rethrow_exception(failure_);
      }
      if (!accepting_) {
        throw std::runtime_error("storage engine is closed");
      }
      queue_.push_back(mutation);
      ++pending_entries_;
      pending_bytes_ += mutation->bytes;
    }
    work_available_.notify_one();
    return future.get();
  }

  void apply_recovered(const WalRecord& record) {
    if (record.operation == WalOperation::put) {
      values_[record.key] = record.value;
    } else {
      values_.erase(record.key);
    }
  }

  std::vector<std::shared_ptr<PendingMutation>> take_batch() {
    std::unique_lock lock(queue_mutex_);
    work_available_.wait(lock, [&] { return stopping_ || !queue_.empty(); });
    if (queue_.empty()) {
      return {};
    }

    if (options_.durability == DurabilityMode::group_commit && !stopping_) {
      const auto deadline = std::chrono::steady_clock::now() + options_.max_batch_wait;
      while (!stopping_ && queue_.size() < options_.max_batch_entries) {
        if (work_available_.wait_until(lock, deadline) == std::cv_status::timeout) {
          break;
        }
      }
    }

    const auto entry_limit =
        options_.durability == DurabilityMode::group_commit
            ? options_.max_batch_entries
            : 1U;
    std::vector<std::shared_ptr<PendingMutation>> batch;
    std::size_t batch_bytes = 0U;
    while (!queue_.empty() && batch.size() < entry_limit) {
      const auto& next = queue_.front();
      if (!batch.empty() && batch_bytes + next->bytes > options_.max_batch_bytes) {
        break;
      }
      batch_bytes += next->bytes;
      pending_bytes_ -= next->bytes;
      --pending_entries_;
      batch.push_back(std::move(queue_.front()));
      queue_.pop_front();
    }
    lock.unlock();
    capacity_available_.notify_all();
    return batch;
  }

  void writer_loop() noexcept {
    for (;;) {
      auto batch = take_batch();
      if (batch.empty()) {
        std::lock_guard lock(queue_mutex_);
        if (stopping_) {
          break;
        }
        continue;
      }
      try {
        process_batch(batch);
      } catch (...) {
        fail(std::current_exception(), batch);
        break;
      }
    }
    wal_.close();
  }

  void process_batch(
      const std::vector<std::shared_ptr<PendingMutation>>& batch) {
    std::vector<WalRecord> records;
    records.reserve(batch.size());
    auto next_lsn = wal_.last_lsn() + 1U;
    std::size_t batch_bytes = 0U;
    for (const auto& mutation : batch) {
      records.push_back(
          {next_lsn++, mutation->operation, mutation->key, mutation->value});
      batch_bytes += mutation->bytes;
      if (options_.hooks.before_append) {
        options_.hooks.before_append(records.back().lsn);
      }
      wal_.append(records.back());
      if (options_.hooks.after_append) {
        options_.hooks.after_append(records.back().lsn);
      }
    }

    if (options_.durability != DurabilityMode::async) {
      const auto final_lsn = records.back().lsn;
      if (options_.hooks.before_sync) {
        options_.hooks.before_sync(final_lsn);
      }
      wal_.sync();
      if (options_.hooks.after_sync) {
        options_.hooks.after_sync(final_lsn);
      }
      if (options_.flush_observer) {
        options_.flush_observer(batch.size(), batch_bytes, final_lsn);
      }
    }

    std::vector<MutationResult> results;
    results.reserve(batch.size());
    {
      std::unique_lock lock(state_mutex_);
      for (std::size_t index = 0U; index < records.size(); ++index) {
        const auto& record = records[index];
        bool existed = values_.contains(record.key);
        if (record.operation == WalOperation::put) {
          values_[record.key] = record.value;
        } else {
          values_.erase(record.key);
        }
        results.push_back({record.lsn, existed});
      }
      visible_lsn_.store(records.back().lsn, std::memory_order_release);
    }
    for (std::size_t index = 0U; index < batch.size(); ++index) {
      batch[index]->completion.set_value(results[index]);
    }
  }

  void fail(
      const std::exception_ptr error,
      const std::vector<std::shared_ptr<PendingMutation>>& active_batch) noexcept {
    for (const auto& mutation : active_batch) {
      try {
        mutation->completion.set_exception(error);
      } catch (...) {
      }
    }

    std::deque<std::shared_ptr<PendingMutation>> abandoned;
    {
      std::lock_guard lock(queue_mutex_);
      failure_ = error;
      accepting_ = false;
      stopping_ = true;
      abandoned.swap(queue_);
      pending_entries_ = 0U;
      pending_bytes_ = 0U;
    }
    for (const auto& mutation : abandoned) {
      try {
        mutation->completion.set_exception(error);
      } catch (...) {
      }
    }
    capacity_available_.notify_all();
  }

  StorageOptions options_;
  Wal wal_;

  mutable std::shared_mutex state_mutex_;
  std::unordered_map<std::string, std::string> values_;
  std::atomic<std::uint64_t> visible_lsn_{0U};

  std::mutex queue_mutex_;
  std::mutex close_mutex_;
  std::condition_variable work_available_;
  std::condition_variable capacity_available_;
  std::deque<std::shared_ptr<PendingMutation>> queue_;
  std::size_t pending_entries_{0U};
  std::size_t pending_bytes_{0U};
  bool accepting_{true};
  bool stopping_{false};
  bool joined_{false};
  std::exception_ptr failure_;
  std::thread writer_;
};

std::unique_ptr<KvStore> KvStore::open(StorageOptions options) {
  validate_options(options);
  auto wal = Wal::open(options.wal_path);
  return std::unique_ptr<KvStore>(
      new KvStore(std::make_unique<Impl>(std::move(options), std::move(wal))));
}

KvStore::KvStore(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

KvStore::~KvStore() = default;

std::uint64_t KvStore::put(std::string key, std::string value) {
  return impl_->put(std::move(key), std::move(value));
}

std::optional<std::string> KvStore::get(const std::string& key) const {
  return impl_->get(key);
}

DeleteResult KvStore::erase(std::string key) {
  return impl_->erase(std::move(key));
}

void KvStore::close() { impl_->close(); }

std::uint64_t KvStore::last_lsn() const noexcept { return impl_->last_lsn(); }

std::size_t KvStore::size() const { return impl_->size(); }

}  // namespace forgekv::storage
