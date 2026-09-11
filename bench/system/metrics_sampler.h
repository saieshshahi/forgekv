#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace forgekv::benchmarking {

struct ProcessSample final {
  std::optional<double> cpu_seconds;
  std::optional<std::uint64_t> rss_bytes;
  std::optional<std::uint64_t> open_fds;
  std::optional<std::uint64_t> threads;
  std::optional<std::uint64_t> read_bytes;
  std::optional<std::uint64_t> write_bytes;
  std::optional<std::uint64_t> voluntary_context_switches;
  std::optional<std::uint64_t> involuntary_context_switches;
};

struct NodeSample final {
  std::uint64_t node{};
  std::int64_t process_id{};
  std::chrono::steady_clock::time_point captured_at;
  ProcessSample process;
  std::optional<std::string> role;
  std::map<std::string, double> metrics;
  std::map<std::string, std::uint64_t> counters;

  [[nodiscard]] std::optional<double> metric(std::string_view name) const;
  [[nodiscard]] std::optional<std::uint64_t> counter(
      std::string_view name) const;
};

struct NodeEndpoint final {
  std::uint64_t node{};
  std::int64_t process_id{};
  std::uint16_t admin_port{};
};

struct SampleResult final {
  std::vector<NodeSample> samples;
  std::string error;
  [[nodiscard]] bool ok() const noexcept { return error.empty(); }
};

struct SampleSeries final {
  std::vector<NodeSample> points;
  std::vector<std::string> diagnostics;
};

[[nodiscard]] std::optional<std::string> parse_proc_status(
    std::string_view text, ProcessSample& sample);
[[nodiscard]] std::optional<std::string> parse_proc_io(
    std::string_view text, ProcessSample& sample);
[[nodiscard]] std::optional<std::string> parse_proc_stat(
    std::string_view text, std::uint64_t ticks_per_second,
    ProcessSample& sample);
[[nodiscard]] std::optional<std::string> parse_prometheus(
    std::string_view text, NodeSample& sample);
[[nodiscard]] std::optional<double> counter_delta(
    std::optional<double> before, std::optional<double> after) noexcept;

class MetricsSampler final {
 public:
  explicit MetricsSampler(
      std::chrono::milliseconds timeout = std::chrono::milliseconds(250));
  [[nodiscard]] SampleResult sample(
      std::span<const NodeEndpoint> endpoints) const;

 private:
  std::chrono::milliseconds timeout_;
};

}  // namespace forgekv::benchmarking
