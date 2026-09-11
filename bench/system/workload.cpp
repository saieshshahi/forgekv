#include "bench/system/workload.h"

#include "protocol/frame.h"

#include <array>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string_view>

namespace forgekv::benchmarking {
namespace {

constexpr std::array<std::size_t, 3> kNodeCounts{1U, 3U, 5U};
constexpr std::array<std::size_t, 5> kConcurrencies{1U, 4U, 16U, 64U, 256U};
constexpr std::array<std::size_t, 4> kValueSizes{100U, 1024U, 4096U, 65536U};
constexpr std::array<WorkloadMix, 5> kMixes{
    WorkloadMix{.reads = 100U, .writes = 0U},
    WorkloadMix{.reads = 95U, .writes = 5U},
    WorkloadMix{.reads = 80U, .writes = 20U},
    WorkloadMix{.reads = 50U, .writes = 50U},
    WorkloadMix{.reads = 0U, .writes = 100U}};

template <typename T, std::size_t Size>
bool contains(const std::array<T, Size>& values, const T value) {
  for (const auto candidate : values) {
    if (candidate == value) return true;
  }
  return false;
}

std::uint64_t mix64(std::uint64_t value) noexcept {
  value += 0x9e3779b97f4a7c15ULL;
  value = (value ^ (value >> 30U)) * 0xbf58476d1ce4e5b9ULL;
  value = (value ^ (value >> 27U)) * 0x94d049bb133111ebULL;
  return value ^ (value >> 31U);
}

void hash_bytes(std::uint64_t& hash, const std::string_view text) noexcept {
  for (const char character : text) {
    const auto byte = static_cast<unsigned char>(character);
    hash ^= byte;
    hash *= 1099511628211ULL;
  }
}

}  // namespace

std::string durability_label(const DurabilityLabel durability) {
  switch (durability) {
    case DurabilityLabel::quorum_sync:
      return "quorum-sync";
    case DurabilityLabel::async:
      return "async";
    case DurabilityLabel::sync:
      return "sync";
    case DurabilityLabel::group_commit:
      return "group-commit";
  }
  return "unknown";
}

std::string scope_label(const BenchmarkScope scope) {
  return scope == BenchmarkScope::cluster ? "cluster" : "storage";
}

std::optional<std::string> validate(const Scenario& scenario) {
  if (scenario.scope != BenchmarkScope::cluster &&
      scenario.scope != BenchmarkScope::storage) {
    return "benchmark scope is unknown";
  }
  if (scenario.scope == BenchmarkScope::cluster) {
    if (!contains(kNodeCounts, scenario.nodes) ||
        scenario.durability != DurabilityLabel::quorum_sync) {
      return "cluster scenarios require 1, 3, or 5 nodes and quorum-sync";
    }
  } else if (scenario.nodes != 1U ||
             scenario.durability == DurabilityLabel::quorum_sync) {
    return "storage scenarios require one node and standalone durability";
  }
  if (scenario.scope == BenchmarkScope::storage &&
      scenario.durability != DurabilityLabel::async &&
      scenario.durability != DurabilityLabel::sync &&
      scenario.durability != DurabilityLabel::group_commit) {
    return "storage durability is unknown";
  }
  if (!contains(kConcurrencies, scenario.concurrency) ||
      !contains(kValueSizes, scenario.value_size) || scenario.key_size == 0U ||
      scenario.key_size > protocol::kMaxKeySize ||
      scenario.key_size + scenario.value_size > protocol::kMaxPayloadSize) {
    return "scenario counts or payload sizes are unsupported";
  }
  bool known_mix = false;
  for (const auto mix : kMixes) known_mix = known_mix || mix == scenario.mix;
  if (!known_mix || static_cast<unsigned>(scenario.mix.reads) +
                        static_cast<unsigned>(scenario.mix.writes) !=
                    100U) {
    return "scenario mix is unsupported";
  }
  if (scenario.warmup.count() <= 0 || scenario.measurement.count() <= 0 ||
      scenario.warmup > std::chrono::hours(1) ||
      scenario.measurement > std::chrono::hours(1) || scenario.trials == 0U ||
      scenario.trials > 100U || scenario.seed == 0U ||
      scenario.pipeline_depth == 0U || scenario.pipeline_depth > 256U) {
    return "scenario timing, trials, seed, or pipeline is unsupported";
  }
  if (scenario.publishable &&
      (scenario.warmup < std::chrono::seconds(10) ||
       scenario.measurement < std::chrono::seconds(30) ||
       scenario.trials < 5U)) {
    return "publishable scenarios require 10s warmup, 30s measurement, and 5 trials";
  }
  return std::nullopt;
}

std::string scenario_id(const Scenario& scenario) {
  std::ostringstream canonical;
  canonical << scope_label(scenario.scope) << '|' << scenario.nodes << '|'
            << scenario.concurrency << '|' << scenario.key_size << '|'
            << scenario.value_size << '|' << static_cast<unsigned>(scenario.mix.reads)
            << '|' << static_cast<unsigned>(scenario.mix.writes) << '|'
            << durability_label(scenario.durability) << '|'
            << scenario.warmup.count() << '|' << scenario.measurement.count()
            << '|' << scenario.trials << '|' << scenario.seed << '|'
            << scenario.pipeline_depth << '|' << scenario.publishable;
  std::uint64_t hash = 1469598103934665603ULL;
  hash_bytes(hash, canonical.str());
  std::ostringstream result;
  result << scope_label(scenario.scope) << '-' << std::hex << std::setw(16)
         << std::setfill('0') << hash;
  return result.str();
}

std::vector<Scenario> required_cluster_matrix() {
  std::vector<Scenario> result;
  result.reserve(kNodeCounts.size() * kConcurrencies.size() *
                 kValueSizes.size() * kMixes.size());
  for (const auto nodes : kNodeCounts) {
    for (const auto concurrency : kConcurrencies) {
      for (const auto value_size : kValueSizes) {
        for (const auto mix : kMixes) {
          result.push_back(Scenario{.scope = BenchmarkScope::cluster,
                                    .nodes = nodes,
                                    .concurrency = concurrency,
                                    .key_size = 16U,
                                    .value_size = value_size,
                                    .mix = mix,
                                    .durability = DurabilityLabel::quorum_sync});
        }
      }
    }
  }
  return result;
}

OperationSelector::OperationSelector(const WorkloadMix mix,
                                     const std::uint64_t seed,
                                     const std::uint64_t worker_index)
    : mix_(mix), offset_(mix64(seed ^ mix64(worker_index)) % 100U) {
  if (static_cast<unsigned>(mix.reads) + static_cast<unsigned>(mix.writes) !=
      100U) {
    throw std::invalid_argument("operation mix must total 100");
  }
}

Operation OperationSelector::next() noexcept {
  constexpr std::uint64_t kCoprimeStride = 37U;
  const auto slot = (position_ * kCoprimeStride + offset_) % 100U;
  ++position_;
  return slot < mix_.reads ? Operation::get : Operation::put;
}

}  // namespace forgekv::benchmarking
