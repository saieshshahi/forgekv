#include "cluster/snapshot_codec.h"
#include "raft/snapshot_store.h"

#include <benchmark/benchmark.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include <unistd.h>

namespace forgekv::cluster {
namespace {

SnapshotState dataset(const std::size_t entries) {
  SnapshotState state;
  state.values.reserve(entries);
  for (std::size_t index = 0; index < entries; ++index) {
    state.values.emplace(
        "snapshot-key-" + std::to_string(index),
        std::vector<std::byte>(64U,
                               static_cast<std::byte>(index & 0xFFU)));
  }
  return state;
}

void SnapshotPauseCopy(benchmark::State& benchmark_state) {
  const auto source = dataset(
      static_cast<std::size_t>(benchmark_state.range(0)));
  for (auto _ : benchmark_state) {
    static_cast<void>(_);
    auto copy = source;
    benchmark::DoNotOptimize(copy);
  }
  benchmark_state.SetItemsProcessed(
      benchmark_state.iterations() * benchmark_state.range(0));
}

void SnapshotEncodeAndPublish(benchmark::State& benchmark_state) {
  static std::atomic<std::uint64_t> sequence{0};
  const auto directory =
      std::filesystem::temp_directory_path() /
      ("forgekv-snapshot-bench-" + std::to_string(::getpid()) + "-" +
       std::to_string(sequence.fetch_add(1)));
  std::filesystem::create_directories(directory);
  const auto source = dataset(
      static_cast<std::size_t>(benchmark_state.range(0)));
  raft::LogIndex index = 1;
  for (auto _ : benchmark_state) {
    static_cast<void>(_);
    raft::StateMachineSnapshot snapshot{
        .last_included_index = index++,
        .last_included_term = 1,
        .state_machine = encode_snapshot_state(source),
    };
    raft::SnapshotStore::write_atomic(directory, 77, 1, 99, snapshot);
    benchmark::ClobberMemory();
  }
  benchmark_state.SetItemsProcessed(
      benchmark_state.iterations() * benchmark_state.range(0));
  std::filesystem::remove_all(directory);
}

BENCHMARK(SnapshotPauseCopy)->Arg(1'000)->Arg(10'000)->Arg(100'000);
BENCHMARK(SnapshotEncodeAndPublish)
    ->Arg(1'000)
    ->Arg(10'000)
    ->Arg(100'000)
    ->Iterations(3);

}  // namespace
}  // namespace forgekv::cluster
