#include "cluster/observability.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <sstream>
#include <string_view>

#include <unistd.h>

namespace forgekv::cluster {
namespace {

constexpr auto kOrder = std::memory_order_relaxed;

std::string_view operation_name(const std::size_t index) {
  constexpr std::array<std::string_view, kRequestOperationCount> names{
      "put", "get", "delete", "ping"};
  return names.at(index);
}

std::string_view outcome_name(const std::size_t index) {
  constexpr std::array<std::string_view, kRequestOutcomeCount> names{
      "ok",       "not_found",       "invalid",
      "redirect", "busy",            "internal",
      "request_id_reuse", "stale_request", "capacity_exceeded"};
  return names.at(index);
}

std::string_view role_name(const raft::Role role) {
  switch (role) {
    case raft::Role::follower:
      return "follower";
    case raft::Role::candidate:
      return "candidate";
    case raft::Role::leader:
      return "leader";
  }
  return "unknown";
}

void render_histogram(std::ostringstream& output, const std::string_view name,
                      const std::string_view labels,
                      const HistogramSnapshot& histogram) {
  std::uint64_t cumulative = 0;
  for (std::size_t index = 0; index < kLatencyBucketsUs.size(); ++index) {
    cumulative += histogram.buckets[index];
    output << name << "_bucket{" << labels;
    if (!labels.empty()) {
      output << ',';
    }
    output << "le=\"" << std::fixed << std::setprecision(6)
           << static_cast<double>(kLatencyBucketsUs[index]) / 1'000'000.0
           << "\"} " << cumulative << '\n';
  }
  output << name << "_bucket{" << labels;
  if (!labels.empty()) {
    output << ',';
  }
  output << "le=\"+Inf\"} " << histogram.count << '\n';
  output << name << "_count";
  if (!labels.empty()) {
    output << '{' << labels << '}';
  }
  output << ' ' << histogram.count << '\n';
  output << name << "_sum";
  if (!labels.empty()) {
    output << '{' << labels << '}';
  }
  output << ' ' << std::fixed << std::setprecision(6)
         << static_cast<double>(histogram.sum_microseconds) / 1'000'000.0
         << '\n';
}

}  // namespace

void AtomicHistogram::observe(const std::chrono::microseconds duration) noexcept {
  const auto nonnegative = std::max(duration.count(), std::int64_t{0});
  const auto value = static_cast<std::uint64_t>(nonnegative);
  const auto found = std::ranges::lower_bound(kLatencyBucketsUs, value);
  if (found != kLatencyBucketsUs.end()) {
    buckets_[static_cast<std::size_t>(found - kLatencyBucketsUs.begin())]
        .fetch_add(1U, kOrder);
  }
  count_.fetch_add(1U, kOrder);
  sum_microseconds_.fetch_add(value, kOrder);
}

HistogramSnapshot AtomicHistogram::snapshot() const noexcept {
  HistogramSnapshot result;
  std::uint64_t finite_count = 0;
  for (std::size_t index = 0; index < result.buckets.size(); ++index) {
    result.buckets[index] = buckets_[index].load(kOrder);
    finite_count += result.buckets[index];
  }
  result.count = std::max(count_.load(kOrder), finite_count);
  result.sum_microseconds = sum_microseconds_.load(kOrder);
  return result;
}

OperationalMetrics::OperationalMetrics(std::vector<raft::NodeId> voters)
    : voters_(std::move(voters)),
      peer_lag_(std::make_unique<std::atomic<std::uint64_t>[]>(voters_.size())),
      peer_lag_visible_(std::make_unique<std::atomic<bool>[]>(voters_.size())) {
  std::ranges::sort(voters_);
  voters_.erase(std::unique(voters_.begin(), voters_.end()), voters_.end());
  for (std::size_t index = 0; index < voters_.size(); ++index) {
    peer_lag_[index].store(0, kOrder);
    peer_lag_visible_[index].store(false, kOrder);
  }
}

void OperationalMetrics::request_started(const RequestOperation operation) noexcept {
  requests_total_[request_operation_index(operation)].fetch_add(1U, kOrder);
  requests_in_flight_.fetch_add(1U, kOrder);
}

void OperationalMetrics::request_finished(
    const RequestOperation operation,
    const std::chrono::microseconds duration) noexcept {
  request_latency_[request_operation_index(operation)].observe(duration);
  requests_in_flight_.fetch_sub(1U, kOrder);
}

void OperationalMetrics::request_outcome(
    const RequestOutcome outcome) noexcept {
  if (outcome != RequestOutcome::ok) {
    errors_total_[request_outcome_index(outcome)].fetch_add(1U, kOrder);
  }
}

void OperationalMetrics::request_rejected(
    const RequestOperation operation) noexcept {
  requests_total_[request_operation_index(operation)].fetch_add(1U, kOrder);
  errors_total_[request_outcome_index(RequestOutcome::busy)].fetch_add(1U,
                                                                       kOrder);
}

void OperationalMetrics::set_queue_depth(const std::size_t depth) noexcept {
  queue_depth_.store(depth, kOrder);
}

void OperationalMetrics::queue_rejected() noexcept {
  queue_rejected_total_.fetch_add(1U, kOrder);
}

void OperationalMetrics::observe_queueing(
    const std::chrono::microseconds duration) noexcept {
  queueing_latency_.observe(duration);
}

void OperationalMetrics::observe_sync(
    const std::chrono::microseconds duration) noexcept {
  sync_latency_.observe(duration);
}

void OperationalMetrics::observe_snapshot(
    const std::chrono::microseconds duration) noexcept {
  snapshot_duration_.observe(duration);
}

void OperationalMetrics::recovery_finished(
    const std::chrono::microseconds duration) noexcept {
  recovery_duration_.observe(duration);
}

void OperationalMetrics::election_started() noexcept {
  elections_total_.fetch_add(1U, kOrder);
}

void OperationalMetrics::leadership_changed() noexcept {
  leadership_changes_total_.fetch_add(1U, kOrder);
}

void OperationalMetrics::append_entries() noexcept {
  append_entries_total_.fetch_add(1U, kOrder);
}

void OperationalMetrics::add_wal_bytes(const std::size_t bytes) noexcept {
  wal_bytes_.fetch_add(bytes, kOrder);
}

void OperationalMetrics::set_raft(const RaftObservation& observation) noexcept {
  raft_sequence_.fetch_add(1U, std::memory_order_acq_rel);
  role_.store(static_cast<std::uint64_t>(observation.role), kOrder);
  term_.store(observation.term, kOrder);
  leader_known_.store(observation.leader_id.has_value(), kOrder);
  leader_id_.store(observation.leader_id.value_or(0), kOrder);
  commit_index_.store(observation.commit_index, kOrder);
  last_applied_.store(observation.last_applied, kOrder);
  last_log_index_.store(observation.last_log_index, kOrder);
  retained_log_records_.store(observation.retained_log_records, kOrder);
  for (std::size_t index = 0; index < voters_.size(); ++index) {
    peer_lag_visible_[index].store(false, kOrder);
  }
  for (const auto& [peer, match] : observation.peer_match) {
    const auto found = std::ranges::lower_bound(voters_, peer);
    if (found == voters_.end() || *found != peer) {
      continue;
    }
    const auto index = static_cast<std::size_t>(found - voters_.begin());
    peer_lag_[index].store(observation.last_log_index > match
                               ? observation.last_log_index - match
                               : 0,
                           kOrder);
    peer_lag_visible_[index].store(true, kOrder);
  }
  raft_sequence_.fetch_add(1U, std::memory_order_release);
}

OperationalMetricsSnapshot OperationalMetrics::snapshot() const {
  OperationalMetricsSnapshot result;
  for (std::size_t index = 0; index < kRequestOperationCount; ++index) {
    result.requests_total[index] = requests_total_[index].load(kOrder);
    result.request_latency[index] = request_latency_[index].snapshot();
  }
  for (std::size_t index = 0; index < kRequestOutcomeCount; ++index) {
    result.errors_total[index] = errors_total_[index].load(kOrder);
  }
  result.queueing_latency = queueing_latency_.snapshot();
  result.sync_latency = sync_latency_.snapshot();
  result.recovery_duration = recovery_duration_.snapshot();
  result.snapshot_duration = snapshot_duration_.snapshot();
  result.requests_in_flight = requests_in_flight_.load(kOrder);
  result.queue_depth = queue_depth_.load(kOrder);
  result.queue_rejected_total = queue_rejected_total_.load(kOrder);
  result.elections_total = elections_total_.load(kOrder);
  result.leadership_changes_total = leadership_changes_total_.load(kOrder);
  result.append_entries_total = append_entries_total_.load(kOrder);
  result.wal_bytes = wal_bytes_.load(kOrder);
  result.sync_count = result.sync_latency.count;
  while (true) {
    const auto before = raft_sequence_.load(std::memory_order_acquire);
    if ((before & 1U) != 0U) {
      continue;
    }
    RaftObservation raft_observation;
    raft_observation.role = static_cast<raft::Role>(role_.load(kOrder));
    raft_observation.term = term_.load(kOrder);
    if (leader_known_.load(kOrder)) {
      raft_observation.leader_id = leader_id_.load(kOrder);
    }
    raft_observation.commit_index = commit_index_.load(kOrder);
    raft_observation.last_applied = last_applied_.load(kOrder);
    raft_observation.last_log_index = last_log_index_.load(kOrder);
    raft_observation.retained_log_records = retained_log_records_.load(kOrder);
    std::unordered_map<raft::NodeId, std::uint64_t> peer_lag;
    for (std::size_t index = 0; index < voters_.size(); ++index) {
      if (peer_lag_visible_[index].load(kOrder)) {
        peer_lag.emplace(voters_[index], peer_lag_[index].load(kOrder));
      }
    }
    std::atomic_thread_fence(std::memory_order_acquire);
    const auto after = raft_sequence_.load(std::memory_order_relaxed);
    if (before == after) {
      result.raft = std::move(raft_observation);
      result.peer_lag = std::move(peer_lag);
      break;
    }
  }
  return result;
}

std::optional<ProcessMetrics> sample_process_metrics() {
  std::ifstream status("/proc/self/status");
  std::ifstream stat("/proc/self/stat");
  if (!status || !stat) {
    return std::nullopt;
  }
  ProcessMetrics result;
  std::string line;
  while (std::getline(status, line)) {
    std::istringstream input(line);
    std::string field;
    input >> field;
    if (field == "VmRSS:") {
      std::uint64_t kibibytes = 0;
      input >> kibibytes;
      result.rss_bytes = kibibytes * 1024U;
    } else if (field == "Threads:") {
      input >> result.threads;
    }
  }

  std::string stat_text;
  std::getline(stat, stat_text);
  const auto close = stat_text.rfind(')');
  if (close == std::string::npos || close + 2U >= stat_text.size()) {
    return std::nullopt;
  }
  std::istringstream fields(stat_text.substr(close + 2U));
  char state = 0;
  fields >> state;
  std::uint64_t ignored = 0;
  for (int field = 4; field <= 13; ++field) {
    fields >> ignored;
  }
  std::uint64_t user_ticks = 0;
  std::uint64_t system_ticks = 0;
  fields >> user_ticks >> system_ticks;
  const auto ticks_per_second = ::sysconf(_SC_CLK_TCK);
  if (!fields || ticks_per_second <= 0 || result.rss_bytes == 0 ||
      result.threads == 0) {
    return std::nullopt;
  }
  result.cpu_seconds = static_cast<double>(user_ticks + system_ticks) /
                       static_cast<double>(ticks_per_second);

  std::error_code error;
  for (std::filesystem::directory_iterator iterator("/proc/self/fd", error),
       end;
       !error && iterator != end; iterator.increment(error)) {
    ++result.open_fds;
  }
  if (error || result.open_fds == 0) {
    return std::nullopt;
  }
  return result;
}

std::string render_prometheus(const OperationalMetricsSnapshot& metrics,
                              const std::optional<ProcessMetrics>& process,
                              const net::MetricsSnapshot& client_network,
                              const net::MetricsSnapshot& peer_network) {
  std::ostringstream output;
  if (process.has_value()) {
    output << "forgekv_process_cpu_seconds_total " << std::fixed
           << std::setprecision(6) << process->cpu_seconds << '\n'
           << "forgekv_process_rss_bytes " << process->rss_bytes << '\n'
           << "forgekv_process_open_fds " << process->open_fds << '\n'
           << "forgekv_process_threads " << process->threads << '\n';
  }
  const auto active_connections = client_network.active_connections +
                                  peer_network.active_connections;
  const auto accepted_connections = client_network.accepted_connections_total +
                                    peer_network.accepted_connections_total;
  output << "forgekv_network_active_connections " << active_connections << '\n'
         << "forgekv_network_connections_total " << accepted_connections << '\n'
         << "forgekv_network_rx_bytes_total "
         << client_network.bytes_read_total + peer_network.bytes_read_total
         << '\n'
         << "forgekv_network_tx_bytes_total "
         << client_network.bytes_written_total + peer_network.bytes_written_total
         << '\n'
         << "forgekv_network_requests_in_flight "
         << client_network.requests_in_flight + peer_network.requests_in_flight
         << '\n'
         << "forgekv_network_backpressured_connections "
         << client_network.connections_backpressured +
                peer_network.connections_backpressured
         << '\n'
         << "forgekv_network_backpressure_events_total "
         << client_network.backpressure_events_total +
                peer_network.backpressure_events_total
         << '\n'
         << "forgekv_network_rejected_requests_total "
         << client_network.rejected_requests_total +
                peer_network.rejected_requests_total
         << '\n';

  for (std::size_t index = 0; index < kRequestOperationCount; ++index) {
    const auto labels = "op=\"" + std::string(operation_name(index)) + "\"";
    output << "forgekv_requests_total{" << labels << "} "
           << metrics.requests_total[index] << '\n';
    render_histogram(output, "forgekv_request_latency_seconds", labels,
                     metrics.request_latency[index]);
  }
  for (std::size_t index = 1; index < kRequestOutcomeCount; ++index) {
    output << "forgekv_errors_total{type=\"" << outcome_name(index) << "\"} "
           << metrics.errors_total[index] << '\n';
  }
  output << "forgekv_requests_in_flight " << metrics.requests_in_flight << '\n'
         << "forgekv_queue_depth " << metrics.queue_depth << '\n'
         << "forgekv_queue_rejected_total "
         << metrics.queue_rejected_total + client_network.rejected_requests_total +
                peer_network.rejected_requests_total
         << '\n';
  render_histogram(output, "forgekv_queueing_latency_seconds", {},
                   metrics.queueing_latency);

  output << "forgekv_raft_role{role=\"" << role_name(metrics.raft.role)
         << "\"} 1\n"
         << "forgekv_raft_term " << metrics.raft.term << '\n'
         << "forgekv_raft_leader_id " << metrics.raft.leader_id.value_or(0)
         << '\n'
         << "forgekv_raft_commit_index " << metrics.raft.commit_index << '\n'
         << "forgekv_raft_last_applied " << metrics.raft.last_applied << '\n'
         << "forgekv_raft_log_size " << metrics.raft.retained_log_records << '\n'
         << "forgekv_raft_elections_total " << metrics.elections_total << '\n'
         << "forgekv_raft_leadership_changes_total "
         << metrics.leadership_changes_total << '\n'
         << "forgekv_raft_append_entries_total "
         << metrics.append_entries_total << '\n';
  for (const auto& [peer, lag] : metrics.peer_lag) {
    output << "forgekv_raft_replication_lag{peer=\"" << peer << "\"} " << lag
           << '\n';
  }

  output << "forgekv_storage_wal_bytes " << metrics.wal_bytes << '\n'
         << "forgekv_storage_wal_records "
         << metrics.raft.retained_log_records << '\n'
         << "forgekv_storage_sync_count_total " << metrics.sync_count << '\n';
  render_histogram(output, "forgekv_storage_sync_latency_seconds", {},
                   metrics.sync_latency);
  render_histogram(output, "forgekv_storage_recovery_duration_seconds", {},
                   metrics.recovery_duration);
  render_histogram(output, "forgekv_storage_snapshot_duration_seconds", {},
                   metrics.snapshot_duration);
  return output.str();
}

}  // namespace forgekv::cluster
