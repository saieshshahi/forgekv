#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace forgekv::benchmarking {

enum class BenchmarkScope : std::uint8_t { cluster, storage };
enum class DurabilityLabel : std::uint8_t {
  quorum_sync,
  async,
  sync,
  group_commit,
};
enum class Operation : std::uint8_t { get, put };

struct WorkloadMix final {
  std::uint8_t reads{};
  std::uint8_t writes{};
  bool operator==(const WorkloadMix&) const = default;
};

struct Scenario final {
  BenchmarkScope scope{BenchmarkScope::cluster};
  std::size_t nodes{3U};
  std::size_t concurrency{1U};
  std::size_t key_size{16U};
  std::size_t value_size{100U};
  WorkloadMix mix{.reads = 100U, .writes = 0U};
  DurabilityLabel durability{DurabilityLabel::quorum_sync};
  std::chrono::milliseconds warmup{std::chrono::seconds(10)};
  std::chrono::milliseconds measurement{std::chrono::seconds(30)};
  std::size_t trials{5U};
  std::uint64_t seed{160016U};
  std::size_t pipeline_depth{1U};
  bool publishable{true};
  bool operator==(const Scenario&) const = default;
};

[[nodiscard]] std::optional<std::string> validate(const Scenario& scenario);
[[nodiscard]] std::string scenario_id(const Scenario& scenario);
[[nodiscard]] std::vector<Scenario> required_cluster_matrix();
[[nodiscard]] std::string durability_label(DurabilityLabel durability);
[[nodiscard]] std::string scope_label(BenchmarkScope scope);

class OperationSelector final {
 public:
  OperationSelector(WorkloadMix mix, std::uint64_t seed,
                    std::uint64_t worker_index);
  [[nodiscard]] Operation next() noexcept;

 private:
  WorkloadMix mix_;
  std::uint64_t position_{};
  std::uint64_t offset_{};
};

}  // namespace forgekv::benchmarking
