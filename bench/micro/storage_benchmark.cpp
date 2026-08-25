#include "storage/kv_store.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <ctime>
#include <filesystem>
#include <future>
#include <string>
#include <vector>

#include <benchmark/benchmark.h>
#include <unistd.h>

namespace forgekv::storage {
namespace {

class BenchmarkDirectory final {
 public:
  BenchmarkDirectory() {
    static std::atomic<std::uint64_t> sequence{0U};
    root_ = std::filesystem::temp_directory_path() /
            ("forgekv-bench-" + std::to_string(::getpid()) + "-" +
             std::to_string(sequence.fetch_add(1U)));
    std::filesystem::create_directories(root_);
  }
  ~BenchmarkDirectory() { std::filesystem::remove_all(root_); }
  [[nodiscard]] std::filesystem::path wal() const { return root_ / "store.wal"; }

 private:
  std::filesystem::path root_;
};

double percentile(std::vector<double> samples, const double fraction) {
  if (samples.empty()) {
    return 0.0;
  }
  std::ranges::sort(samples);
  const auto index = static_cast<std::size_t>(
      std::ceil(fraction * static_cast<double>(samples.size())) - 1.0);
  return samples[std::min(index, samples.size() - 1U)];
}

void report(benchmark::State& state, const std::vector<double>& latencies_us,
            const double bytes_per_operation, const std::clock_t cpu_start,
            const std::chrono::steady_clock::time_point wall_start) {
  const auto wall_seconds = std::chrono::duration<double>(
                                std::chrono::steady_clock::now() - wall_start)
                                .count();
  const auto cpu_seconds = static_cast<double>(std::clock() - cpu_start) /
                           static_cast<double>(CLOCKS_PER_SEC);
  double operation_seconds = 0.0;
  for (const auto latency : latencies_us) {
    operation_seconds += latency / 1'000'000.0;
  }
  const auto operations = static_cast<double>(latencies_us.size());
  const auto rate = operation_seconds > 0.0 ? operations / operation_seconds : 0.0;
  state.counters["ops_per_sec"] = rate;
  state.counters["bytes_per_sec"] = rate * static_cast<double>(bytes_per_operation);
  state.counters["p50_us"] = percentile(latencies_us, 0.50);
  state.counters["p95_us"] = percentile(latencies_us, 0.95);
  state.counters["p99_us"] = percentile(latencies_us, 0.99);
  state.counters["max_us"] = latencies_us.empty()
                                 ? 0.0
                                 : *std::ranges::max_element(latencies_us);
  state.counters["cpu_utilization_pct"] =
      wall_seconds > 0.0 ? (cpu_seconds / wall_seconds) * 100.0 : 0.0;
  state.SetItemsProcessed(static_cast<std::int64_t>(latencies_us.size()));
  state.SetBytesProcessed(static_cast<std::int64_t>(
      static_cast<double>(latencies_us.size()) * bytes_per_operation));
}

StorageOptions async_options(const std::filesystem::path& path) {
  StorageOptions options;
  options.wal_path = path;
  options.durability = DurabilityMode::async;
  return options;
}

void StoragePut(benchmark::State& state) {
  const auto value_size = static_cast<std::size_t>(state.range(0));
  const auto key_count = static_cast<std::size_t>(state.range(1));
  BenchmarkDirectory directory;
  auto store = KvStore::open(async_options(directory.wal()));
  const std::string value(value_size, 'v');
  std::vector<double> latencies;
  std::size_t index = 0U;
  const auto cpu_start = std::clock();
  const auto wall_start = std::chrono::steady_clock::now();
  for (auto _ : state) {
    static_cast<void>(_);
    const auto key = "key-" + std::to_string(index++ % key_count);
    const auto begin = std::chrono::steady_clock::now();
    benchmark::DoNotOptimize(store->put(key, value));
    const auto elapsed = std::chrono::steady_clock::now() - begin;
    state.SetIterationTime(std::chrono::duration<double>(elapsed).count());
    latencies.push_back(std::chrono::duration<double, std::micro>(elapsed).count());
  }
  report(state, latencies, static_cast<double>(value_size), cpu_start, wall_start);
}

void StorageGet(benchmark::State& state) {
  const auto value_size = static_cast<std::size_t>(state.range(0));
  const auto key_count = static_cast<std::size_t>(state.range(1));
  BenchmarkDirectory directory;
  auto store = KvStore::open(async_options(directory.wal()));
  const std::string value(value_size, 'v');
  for (std::size_t index = 0U; index < key_count; ++index) {
    static_cast<void>(store->put("key-" + std::to_string(index), value));
  }
  std::vector<double> latencies;
  std::size_t index = 0U;
  const auto cpu_start = std::clock();
  const auto wall_start = std::chrono::steady_clock::now();
  for (auto _ : state) {
    static_cast<void>(_);
    const auto key = "key-" + std::to_string(index++ % key_count);
    const auto begin = std::chrono::steady_clock::now();
    benchmark::DoNotOptimize(store->get(key));
    const auto elapsed = std::chrono::steady_clock::now() - begin;
    state.SetIterationTime(std::chrono::duration<double>(elapsed).count());
    latencies.push_back(std::chrono::duration<double, std::micro>(elapsed).count());
  }
  report(state, latencies, static_cast<double>(value_size), cpu_start, wall_start);
}

void StorageDelete(benchmark::State& state) {
  const auto value_size = static_cast<std::size_t>(state.range(0));
  const auto key_count = static_cast<std::size_t>(state.range(1));
  BenchmarkDirectory directory;
  auto store = KvStore::open(async_options(directory.wal()));
  const std::string value(value_size, 'v');
  double average_key_bytes = 0.0;
  for (std::size_t key_index = 0U; key_index < key_count; ++key_index) {
    average_key_bytes +=
        static_cast<double>(("key-" + std::to_string(key_index)).size());
  }
  average_key_bytes /= static_cast<double>(key_count);
  std::vector<double> latencies;
  std::size_t index = 0U;
  const auto cpu_start = std::clock();
  const auto wall_start = std::chrono::steady_clock::now();
  for (auto _ : state) {
    static_cast<void>(_);
    const auto key = "key-" + std::to_string(index++ % key_count);
    state.PauseTiming();
    static_cast<void>(store->put(key, value));
    state.ResumeTiming();
    const auto begin = std::chrono::steady_clock::now();
    benchmark::DoNotOptimize(store->erase(key));
    const auto elapsed = std::chrono::steady_clock::now() - begin;
    state.SetIterationTime(std::chrono::duration<double>(elapsed).count());
    latencies.push_back(std::chrono::duration<double, std::micro>(elapsed).count());
  }
  report(state, latencies, average_key_bytes, cpu_start, wall_start);
}

void StorageRecovery(benchmark::State& state) {
  const auto value_size = static_cast<std::size_t>(state.range(0));
  const auto key_count = static_cast<std::size_t>(state.range(1));
  BenchmarkDirectory directory;
  {
    auto source = KvStore::open(async_options(directory.wal()));
    const std::string value(value_size, 'v');
    for (std::size_t index = 0U; index < key_count; ++index) {
      static_cast<void>(source->put("key-" + std::to_string(index), value));
    }
  }
  std::vector<double> latencies;
  const auto cpu_start = std::clock();
  const auto wall_start = std::chrono::steady_clock::now();
  for (auto _ : state) {
    static_cast<void>(_);
    const auto begin = std::chrono::steady_clock::now();
    auto recovered = KvStore::open(async_options(directory.wal()));
    benchmark::DoNotOptimize(recovered->size());
    const auto elapsed = std::chrono::steady_clock::now() - begin;
    state.SetIterationTime(std::chrono::duration<double>(elapsed).count());
    latencies.push_back(std::chrono::duration<double, std::micro>(elapsed).count());
  }
  report(state, latencies, static_cast<double>(value_size * key_count), cpu_start,
         wall_start);
}

void StorageDurabilityPut(benchmark::State& state) {
  const auto mode_value = state.range(0);
  const auto mode = mode_value == 0   ? DurabilityMode::async
                    : mode_value == 1 ? DurabilityMode::sync
                                      : DurabilityMode::group_commit;
  const auto value_size = static_cast<std::size_t>(state.range(1));
  const auto concurrency = static_cast<std::size_t>(state.range(2));
  BenchmarkDirectory directory;
  StorageOptions options;
  options.wal_path = directory.wal();
  options.durability = mode;
  options.max_batch_entries = concurrency;
  options.max_batch_wait = std::chrono::milliseconds(2);
  auto store = KvStore::open(std::move(options));
  const std::string value(value_size, 'v');
  std::vector<double> latencies;
  double batch_seconds = 0.0;
  std::size_t sequence = 0U;
  const auto cpu_start = std::clock();
  const auto wall_start = std::chrono::steady_clock::now();
  for (auto _ : state) {
    static_cast<void>(_);
    std::vector<std::future<double>> writes;
    writes.reserve(concurrency);
    const auto batch_begin = std::chrono::steady_clock::now();
    for (std::size_t worker = 0U; worker < concurrency; ++worker) {
      const auto key = "key-" + std::to_string(sequence++);
      writes.push_back(std::async(std::launch::async, [&, key] {
        const auto begin = std::chrono::steady_clock::now();
        static_cast<void>(store->put(key, value));
        return std::chrono::duration<double, std::micro>(
                   std::chrono::steady_clock::now() - begin)
            .count();
      }));
    }
    for (auto& write : writes) {
      latencies.push_back(write.get());
    }
    const auto elapsed = std::chrono::steady_clock::now() - batch_begin;
    const auto elapsed_seconds = std::chrono::duration<double>(elapsed).count();
    batch_seconds += elapsed_seconds;
    state.SetIterationTime(elapsed_seconds);
  }
  report(state, latencies, static_cast<double>(value_size), cpu_start, wall_start);
  const auto operations = static_cast<double>(latencies.size());
  const auto rate = batch_seconds > 0.0 ? operations / batch_seconds : 0.0;
  state.counters["ops_per_sec"] = rate;
  state.counters["bytes_per_sec"] = rate * static_cast<double>(value_size);
}

constexpr int kValueSizes[] = {100, 1024, 4096, 65536, 1048576};

void register_matrix(benchmark::internal::Benchmark* benchmark) {
  for (const auto value_size : kValueSizes) {
    for (const int key_count : {1, 64}) {
      benchmark->Args({value_size, key_count});
    }
  }
}

void register_durability_matrix(benchmark::internal::Benchmark* benchmark) {
  for (const int mode : {0, 1, 2}) {
    for (const int value_size : {100, 4096}) {
      benchmark->Args({mode, value_size, 8});
    }
  }
}

BENCHMARK(StoragePut)->Apply(register_matrix)->Iterations(100)->UseManualTime();
BENCHMARK(StorageGet)->Apply(register_matrix)->Iterations(100)->UseManualTime();
BENCHMARK(StorageDelete)->Apply(register_matrix)->Iterations(100)->UseManualTime();
BENCHMARK(StorageRecovery)->Apply(register_matrix)->Iterations(5)->UseManualTime();
BENCHMARK(StorageDurabilityPut)
    ->Apply(register_durability_matrix)
    ->ArgNames({"mode_0_async_1_sync_2_group", "value_bytes", "concurrency"})
    ->Iterations(20)
    ->UseManualTime();

}  // namespace
}  // namespace forgekv::storage
