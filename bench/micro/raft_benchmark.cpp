#include "raft/persisted_raft_node.h"

#include <benchmark/benchmark.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <unistd.h>

namespace forgekv::raft {
namespace {

class BenchmarkDirectory final {
 public:
  BenchmarkDirectory() {
    static std::atomic<std::uint64_t> sequence{0};
    path_ = std::filesystem::temp_directory_path() /
            ("forgekv-raft-bench-" + std::to_string(::getpid()) + "-" +
             std::to_string(sequence.fetch_add(1U)));
    std::filesystem::create_directories(path_);
  }
  ~BenchmarkDirectory() { std::filesystem::remove_all(path_); }
  [[nodiscard]] const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

AppendEntries append_to(const std::vector<Action>& actions,
                        const NodeId peer) {
  const auto found = std::ranges::find_if(actions, [peer](const Action& action) {
    const auto* send = std::get_if<SendMessage>(&action);
    return send != nullptr && send->to == peer &&
           std::holds_alternative<AppendEntries>(send->message);
  });
  if (found == actions.end()) {
    throw std::logic_error("benchmark did not produce AppendEntries");
  }
  return std::get<AppendEntries>(std::get<SendMessage>(*found).message);
}

void LocalReadBaseline(benchmark::State& state) {
  const std::unordered_map<std::string, std::string> data{{"key", "value"}};
  for (auto _ : state) {
    static_cast<void>(_);
    benchmark::DoNotOptimize(data.find("key"));
  }
  state.SetItemsProcessed(state.iterations());
}

void DurableReadBarrierLowerBound(benchmark::State& state) {
  BenchmarkDirectory directory;
  std::vector<Action> output;
  auto node = PersistedRaftNode::open(PersistedRaftOptions{
      .config = RaftConfig{.self_id = 1,
                           .cluster_id = 4242,
                           .voters = {1, 2, 3},
                           .election_timeout_min = 100,
                           .election_timeout_max = 200,
                           .heartbeat_interval = 25,
                           .random_seed = 9},
      .data_directory = directory.path(),
      .initial_time = 0,
      .output = [&output](const Action& action) { output.push_back(action); },
      .crash_hook = {},
  });
  node.advance_time(node.snapshot().election_deadline);
  node.step(2, RequestVoteResponse{.term = 1, .vote_granted = true});
  const auto initial = append_to(output, 2);
  node.step(2, AppendEntriesResponse{
                   .term = 1,
                   .success = true,
                   .match_index = initial.previous_log_index +
                                  initial.entries.size(),
                   .reject_hint = 0,
                   .rpc_id = initial.rpc_id});

  for (auto _ : state) {
    static_cast<void>(_);
    output.clear();
    node.read_barrier();
    const auto request = append_to(output, 2);
    const auto last_index = request.previous_log_index + request.entries.size();
    node.step(2, AppendEntriesResponse{.term = 1,
                                       .success = true,
                                       .match_index = last_index,
                                       .reject_hint = 0,
                                       .rpc_id = request.rpc_id});
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(LocalReadBaseline)->Iterations(1'000);
BENCHMARK(DurableReadBarrierLowerBound)->Iterations(100);

}  // namespace
}  // namespace forgekv::raft
