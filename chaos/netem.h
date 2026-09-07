#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace forgekv::chaos {

struct NetemProfile final {
  std::string name;
  std::uint32_t delay_us{};
  std::uint32_t jitter_us{};
  std::uint32_t loss_basis_points{};
  std::uint32_t reorder_basis_points{};
  std::uint32_t correlation_basis_points{};
  bool operator==(const NetemProfile&) const = default;
};

struct NetemArguments final {
  std::vector<std::string> arguments;
  std::string error;
  [[nodiscard]] bool ok() const noexcept { return error.empty(); }
};

struct ChaosRunSummary final {
  bool passed{};
  bool converged{};
  bool restart_verified{};
  std::uint64_t attempts{};
  std::uint64_t acknowledged_writes{};
  std::uint64_t actions{};
};

struct ChaosSummaryParse final {
  std::optional<ChaosRunSummary> summary;
  std::string error;
  [[nodiscard]] bool ok() const noexcept {
    return error.empty() && summary.has_value();
  }
};

struct QdiscStats final {
  std::uint64_t packets{};
  std::uint64_t dropped{};
};

struct QdiscStatsParse final {
  std::optional<QdiscStats> stats;
  std::string error;
  [[nodiscard]] bool ok() const noexcept {
    return error.empty() && stats.has_value();
  }
};

[[nodiscard]] std::vector<NetemProfile> required_netem_profiles();
[[nodiscard]] std::optional<NetemProfile> named_netem_profile(
    std::string_view name);
[[nodiscard]] NetemArguments make_netem_arguments(const NetemProfile& profile);
[[nodiscard]] std::string netem_fault_label(const NetemProfile& profile);
[[nodiscard]] std::string proxy_fault_label(bool partitioned);
[[nodiscard]] ChaosSummaryParse parse_chaos_output(std::string_view output);
[[nodiscard]] QdiscStatsParse parse_qdisc_stats(std::string_view output);

}  // namespace forgekv::chaos
