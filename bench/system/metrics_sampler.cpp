#include "bench/system/metrics_sampler.h"

#include "chaos/verifier.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <limits>
#include <set>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <utility>

#include <unistd.h>

namespace forgekv::benchmarking {
namespace {

constexpr std::size_t kMaximumSampleBytes = 64U * 1024U;

std::string_view trim(std::string_view value) {
  while (!value.empty() && (value.front() == ' ' || value.front() == '\t' ||
                            value.front() == '\r')) {
    value.remove_prefix(1U);
  }
  while (!value.empty() && (value.back() == ' ' || value.back() == '\t' ||
                            value.back() == '\r')) {
    value.remove_suffix(1U);
  }
  return value;
}

bool parse_u64(const std::string_view text, std::uint64_t& value) {
  if (text.empty() || text.front() == '-') return false;
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), value);
  return error == std::errc{} && end == text.data() + text.size();
}

bool parse_nonnegative_double(const std::string_view text, double& value) {
  if (text.empty() || text.front() == '-') return false;
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), value);
  return error == std::errc{} && end == text.data() + text.size() &&
         std::isfinite(value) && value >= 0.0;
}

template <typename Assign>
std::optional<std::string> parse_status_field(
    const std::string_view line, const std::string_view name, bool& seen,
    Assign assign) {
  if (!line.starts_with(name)) return std::nullopt;
  if (seen) return std::string("duplicate ") + std::string(name);
  auto value_text = trim(line.substr(name.size()));
  const auto separator = value_text.find_first_of(" \t");
  const auto number = value_text.substr(0U, separator);
  std::uint64_t value = 0U;
  if (!parse_u64(number, value)) {
    return std::string("invalid ") + std::string(name);
  }
  const auto unit = separator == std::string_view::npos
                        ? std::string_view{}
                        : trim(value_text.substr(separator));
  if (!assign(value, unit)) {
    return std::string("invalid ") + std::string(name);
  }
  seen = true;
  return std::string{};
}

std::optional<std::string> read_bounded(const std::filesystem::path& path,
                                        std::string& output) {
  std::ifstream input(path, std::ios::binary);
  if (!input) return "open " + path.string() + " failed";
  output.clear();
  std::array<char, 4096> buffer{};
  while (input) {
    input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
    const auto count = input.gcount();
    if (count <= 0) break;
    const auto size = static_cast<std::size_t>(count);
    if (size > kMaximumSampleBytes - output.size()) {
      return "read " + path.string() + " exceeded 64 KiB";
    }
    output.append(buffer.data(), size);
  }
  if (input.bad()) return "read " + path.string() + " failed";
  return std::nullopt;
}

struct ParsedMetricName final {
  std::string_view base;
  std::map<std::string, std::string> labels;
};

std::optional<std::string> parse_metric_name(const std::string_view name,
                                             ParsedMetricName& parsed) {
  const auto brace = name.find('{');
  if (brace == std::string_view::npos) {
    if (name.empty() || name.find('}') != std::string_view::npos) {
      return "malformed metric name";
    }
    parsed.base = name;
    return std::nullopt;
  }
  if (brace == 0U || !name.ends_with('}')) return "malformed metric labels";
  parsed.base = name.substr(0U, brace);
  auto labels = name.substr(brace + 1U, name.size() - brace - 2U);
  while (!labels.empty()) {
    const auto equals = labels.find("=\"");
    if (equals == std::string_view::npos || equals == 0U) {
      return "malformed metric label";
    }
    const auto end_quote = labels.find('"', equals + 2U);
    if (end_quote == std::string_view::npos) return "malformed metric label";
    const std::string key(labels.substr(0U, equals));
    const std::string value(labels.substr(equals + 2U,
                                          end_quote - equals - 2U));
    if (!parsed.labels.emplace(key, value).second) {
      return "duplicate metric label";
    }
    labels.remove_prefix(end_quote + 1U);
    if (labels.empty()) break;
    if (labels.front() != ',') return "malformed metric label separator";
    labels.remove_prefix(1U);
  }
  return std::nullopt;
}

bool one_of(const std::string& value,
            const std::initializer_list<std::string_view> choices) {
  return std::ranges::any_of(choices, [&](const auto choice) {
    return value == choice;
  });
}

bool is_histogram_base(const std::string_view base) {
  constexpr std::array<std::string_view, 4> prefixes{
      "forgekv_request_latency_seconds", "forgekv_queueing_latency_seconds",
      "forgekv_storage_sync_latency_seconds",
      "forgekv_storage_recovery_duration_seconds"};
  const auto matches = [&](const std::string_view prefix) {
    if (!base.starts_with(prefix)) return false;
    const auto suffix = base.substr(prefix.size());
    return suffix == "_bucket" || suffix == "_count" || suffix == "_sum";
  };
  if (matches("forgekv_storage_snapshot_duration_seconds")) return true;
  return std::ranges::any_of(prefixes, [&](const auto prefix) {
    return matches(prefix);
  });
}

bool is_floating_metric(const std::string_view base) {
  return base == "forgekv_process_cpu_seconds_total" || base.ends_with("_sum");
}

std::optional<std::string> validate_labels(const ParsedMetricName& parsed) {
  const auto& base = parsed.base;
  const auto& labels = parsed.labels;
  if (base == "forgekv_raft_role") {
    const auto role = labels.find("role");
    if (labels.size() != 1U || role == labels.end() ||
        !one_of(role->second, {"leader", "candidate", "follower"})) {
      return "invalid Raft role labels";
    }
    return std::nullopt;
  }
  if (base == "forgekv_raft_replication_lag") {
    const auto peer = labels.find("peer");
    std::uint64_t peer_id = 0U;
    if (labels.size() != 1U || peer == labels.end() ||
        !parse_u64(peer->second, peer_id) || peer_id == 0U) {
      return "invalid replication lag labels";
    }
    return std::nullopt;
  }
  if (base == "forgekv_requests_total") {
    const auto op = labels.find("op");
    if (labels.size() != 1U || op == labels.end() ||
        !one_of(op->second, {"put", "get", "delete", "ping"})) {
      return "invalid request labels";
    }
    return std::nullopt;
  }
  if (base == "forgekv_errors_total") {
    const auto type = labels.find("type");
    if (labels.size() != 1U || type == labels.end() ||
        !one_of(type->second,
                {"not_found", "invalid", "redirect", "busy", "internal",
                 "request_id_reuse", "stale_request", "capacity_exceeded"})) {
      return "invalid error labels";
    }
    return std::nullopt;
  }
  if (is_histogram_base(base)) {
    const bool request = base.starts_with("forgekv_request_latency_seconds");
    const bool bucket = base.ends_with("_bucket");
    const auto op = labels.find("op");
    const auto le = labels.find("le");
    const auto expected = static_cast<std::size_t>(request) +
                          static_cast<std::size_t>(bucket);
    if (labels.size() != expected || (request && op == labels.end()) ||
        (!request && op != labels.end()) || (bucket && le == labels.end()) ||
        (!bucket && le != labels.end())) {
      return "invalid histogram labels";
    }
    if (request && !one_of(op->second, {"put", "get", "delete", "ping"})) {
      return "invalid histogram operation label";
    }
    double boundary = 0.0;
    if (bucket && le->second != "+Inf" &&
        !parse_nonnegative_double(le->second, boundary)) {
      return "invalid histogram boundary";
    }
    return std::nullopt;
  }
  if (base.starts_with("forgekv_") && !labels.empty()) {
    return "unexpected labels on ForgeKV metric";
  }
  return std::nullopt;
}

}  // namespace

std::optional<double> NodeSample::metric(const std::string_view name) const {
  const auto found = metrics.find(std::string(name));
  return found == metrics.end() ? std::nullopt
                                : std::optional<double>(found->second);
}

std::optional<std::uint64_t> NodeSample::counter(
    const std::string_view name) const {
  const auto found = counters.find(std::string(name));
  return found == counters.end()
             ? std::nullopt
             : std::optional<std::uint64_t>(found->second);
}

std::optional<std::string> parse_proc_status(const std::string_view text,
                                             ProcessSample& sample) {
  if (text.empty() || text.size() > kMaximumSampleBytes) {
    return "invalid /proc status size";
  }
  ProcessSample parsed = sample;
  bool rss_seen = false;
  bool threads_seen = false;
  bool voluntary_seen = false;
  bool involuntary_seen = false;
  bool recognized = false;
  std::size_t offset = 0U;
  while (offset < text.size()) {
    const auto end = text.find('\n', offset);
    const auto line = trim(text.substr(
        offset, end == std::string_view::npos ? text.size() - offset
                                              : end - offset));
    offset = end == std::string_view::npos ? text.size() : end + 1U;
    const auto parse = [&](const std::string_view name, bool& seen,
                           const auto assign) -> std::optional<std::string> {
      auto result = parse_status_field(line, name, seen, assign);
      if (result.has_value()) recognized = true;
      return result;
    };
    if (auto result = parse("VmRSS:", rss_seen,
                            [&](const std::uint64_t value,
                                const std::string_view unit) {
                              if (unit != "kB" ||
                                  value > std::numeric_limits<std::uint64_t>::max() /
                                              1024U) {
                                return false;
                              }
                              parsed.rss_bytes = value * 1024U;
                              return true;
                            });
        result.has_value()) {
      if (!result->empty()) return result;
      continue;
    }
    if (auto result = parse("Threads:", threads_seen,
                            [&](const std::uint64_t value,
                                const std::string_view unit) {
                              if (!unit.empty()) return false;
                              parsed.threads = value;
                              return true;
                            });
        result.has_value()) {
      if (!result->empty()) return result;
      continue;
    }
    if (auto result = parse("voluntary_ctxt_switches:", voluntary_seen,
                            [&](const std::uint64_t value,
                                const std::string_view unit) {
                              if (!unit.empty()) return false;
                              parsed.voluntary_context_switches = value;
                              return true;
                            });
        result.has_value()) {
      if (!result->empty()) return result;
      continue;
    }
    if (auto result = parse("nonvoluntary_ctxt_switches:", involuntary_seen,
                            [&](const std::uint64_t value,
                                const std::string_view unit) {
                              if (!unit.empty()) return false;
                              parsed.involuntary_context_switches = value;
                              return true;
                            });
        result.has_value()) {
      if (!result->empty()) return result;
    }
  }
  if (!recognized) return "no recognized /proc status fields";
  sample = std::move(parsed);
  return std::nullopt;
}

std::optional<std::string> parse_proc_io(const std::string_view text,
                                         ProcessSample& sample) {
  if (text.empty() || text.size() > kMaximumSampleBytes) {
    return "invalid /proc io size";
  }
  ProcessSample parsed = sample;
  bool read_seen = false;
  bool write_seen = false;
  bool recognized = false;
  std::size_t offset = 0U;
  while (offset < text.size()) {
    const auto end = text.find('\n', offset);
    const auto line = trim(text.substr(
        offset, end == std::string_view::npos ? text.size() - offset
                                              : end - offset));
    offset = end == std::string_view::npos ? text.size() : end + 1U;
    const auto parse = [&](const std::string_view name, bool& seen,
                           std::optional<std::uint64_t>& destination) {
      auto result = parse_status_field(
          line, name, seen,
          [&](const std::uint64_t value, const std::string_view unit) {
            if (!unit.empty()) return false;
            destination = value;
            return true;
          });
      if (result.has_value()) recognized = true;
      return result;
    };
    if (auto result = parse("read_bytes:", read_seen, parsed.read_bytes);
        result.has_value()) {
      if (!result->empty()) return result;
      continue;
    }
    if (auto result = parse("write_bytes:", write_seen, parsed.write_bytes);
        result.has_value()) {
      if (!result->empty()) return result;
    }
  }
  if (!recognized) return "no recognized /proc io fields";
  sample = std::move(parsed);
  return std::nullopt;
}

std::optional<std::string> parse_proc_stat(const std::string_view text,
                                           const std::uint64_t ticks_per_second,
                                           ProcessSample& sample) {
  if (text.empty() || text.size() > kMaximumSampleBytes ||
      ticks_per_second == 0U) {
    return "invalid /proc stat input";
  }
  const auto open = text.find('(');
  const auto close = text.rfind(')');
  std::uint64_t pid = 0U;
  if (open == std::string_view::npos || close == std::string_view::npos ||
      close <= open || !parse_u64(trim(text.substr(0U, open)), pid) || pid == 0U) {
    return "malformed /proc stat identity";
  }
  std::istringstream fields(std::string(text.substr(close + 1U)));
  std::vector<std::string> values;
  for (std::string value; fields >> value;) values.push_back(std::move(value));
  if (values.size() <= 12U) return "truncated /proc stat";
  std::uint64_t user_ticks = 0U;
  std::uint64_t system_ticks = 0U;
  if (!parse_u64(values[11U], user_ticks) ||
      !parse_u64(values[12U], system_ticks) ||
      system_ticks > std::numeric_limits<std::uint64_t>::max() - user_ticks) {
    return "invalid /proc stat CPU counters";
  }
  sample.cpu_seconds = static_cast<double>(user_ticks + system_ticks) /
                       static_cast<double>(ticks_per_second);
  return std::nullopt;
}

std::optional<std::string> parse_prometheus(const std::string_view text,
                                            NodeSample& sample) {
  if (text.empty() || text.size() > kMaximumSampleBytes) {
    return "invalid Prometheus sample size";
  }
  NodeSample parsed = sample;
  std::size_t offset = 0U;
  while (offset < text.size()) {
    const auto end = text.find('\n', offset);
    const auto line = trim(text.substr(
        offset, end == std::string_view::npos ? text.size() - offset
                                              : end - offset));
    offset = end == std::string_view::npos ? text.size() : end + 1U;
    if (line.empty() || line.starts_with('#')) continue;
    const auto separator = line.find_first_of(" \t");
    if (separator == std::string_view::npos) return "malformed metric line";
    const auto name = line.substr(0U, separator);
    const auto value_text = trim(line.substr(separator));
    if (value_text.find_first_of(" \t") != std::string_view::npos) {
      return "unexpected Prometheus timestamp";
    }
    ParsedMetricName metric;
    if (auto error = parse_metric_name(name, metric); error.has_value()) {
      return error;
    }
    if (auto error = validate_labels(metric); error.has_value()) return error;
    double value = 0.0;
    std::optional<std::uint64_t> counter_value;
    if (is_floating_metric(metric.base)) {
      if (!parse_nonnegative_double(value_text, value)) {
        return "invalid Prometheus floating value";
      }
    } else {
      std::uint64_t integer = 0U;
      if (!parse_u64(value_text, integer)) {
        return "invalid or overflowing Prometheus counter";
      }
      counter_value = integer;
      value = static_cast<double>(integer);
    }
    const bool forgekv_metric = metric.base.starts_with("forgekv_");
    if (forgekv_metric &&
        !parsed.metrics.emplace(std::string(name), value).second) {
      return "duplicate Prometheus metric";
    }
    if (forgekv_metric && counter_value.has_value()) {
      parsed.counters.emplace(std::string(name), *counter_value);
    }
    if (metric.base == "forgekv_raft_role") {
      if (value != 1.0 || parsed.role.has_value()) {
        return "invalid or duplicate active Raft role";
      }
      parsed.role = metric.labels.at("role");
    }
  }
  sample = std::move(parsed);
  return std::nullopt;
}

std::optional<double> counter_delta(const std::optional<double> before,
                                    const std::optional<double> after) noexcept {
  if (!before.has_value() || !after.has_value() || !std::isfinite(*before) ||
      !std::isfinite(*after) || *before < 0.0 || *after < *before) {
    return std::nullopt;
  }
  return *after - *before;
}

MetricsSampler::MetricsSampler(const std::chrono::milliseconds timeout)
    : timeout_(timeout) {
  if (timeout_.count() <= 0) {
    throw std::invalid_argument("metrics timeout must be positive");
  }
}

SampleResult MetricsSampler::sample(
    const std::span<const NodeEndpoint> endpoints) const {
  SampleResult result;
  if (endpoints.empty()) {
    result.error = "metrics sample requires at least one endpoint";
    return result;
  }
  std::set<std::uint64_t> nodes;
  for (const auto endpoint : endpoints) {
    if (endpoint.node == 0U || endpoint.process_id <= 0 ||
        endpoint.admin_port == 0U || !nodes.insert(endpoint.node).second) {
      result.error = "invalid or duplicate metrics endpoint";
      return result;
    }
    NodeSample node{.node = endpoint.node,
                    .process_id = endpoint.process_id,
                    .captured_at = std::chrono::steady_clock::now(),
                    .process = {},
                    .role = std::nullopt,
                    .metrics = {},
                    .counters = {}};
    const auto root = std::filesystem::path("/proc") /
                      std::to_string(endpoint.process_id);
    std::string text;
    if (auto error = read_bounded(root / "status", text); error.has_value() ||
        (error = parse_proc_status(text, node.process)).has_value()) {
      result.error = error.value_or("parse /proc status failed");
      return result;
    }
    if (auto error = read_bounded(root / "io", text); error.has_value() ||
        (error = parse_proc_io(text, node.process)).has_value()) {
      result.error = error.value_or("parse /proc io failed");
      return result;
    }
    if (auto error = read_bounded(root / "stat", text); error.has_value()) {
      result.error = *error;
      return result;
    }
    const auto ticks = ::sysconf(_SC_CLK_TCK);
    if (ticks <= 0 ||
        parse_proc_stat(text, static_cast<std::uint64_t>(ticks), node.process)
            .has_value()) {
      result.error = "parse /proc stat failed";
      return result;
    }
    std::error_code directory_error;
    std::uint64_t descriptors = 0U;
    for (std::filesystem::directory_iterator iterator(root / "fd",
                                                       directory_error),
         end;
         !directory_error && iterator != end; iterator.increment(directory_error)) {
      if (descriptors == std::numeric_limits<std::uint64_t>::max()) {
        directory_error = std::make_error_code(std::errc::value_too_large);
        break;
      }
      ++descriptors;
    }
    if (directory_error) {
      result.error = "enumerate /proc fd failed";
      return result;
    }
    node.process.open_fds = descriptors;
    const auto admin = chaos::fetch_admin_text(
        {.node = endpoint.node, .port = endpoint.admin_port}, "/metrics",
        timeout_);
    if (!admin.ok() || admin.status != 200 ||
        admin.body.size() > kMaximumSampleBytes) {
      result.error = "node metrics endpoint unavailable or oversized";
      return result;
    }
    if (auto error = parse_prometheus(admin.body, node); error.has_value()) {
      result.error = *error;
      return result;
    }
    result.samples.push_back(std::move(node));
  }
  return result;
}

}  // namespace forgekv::benchmarking
