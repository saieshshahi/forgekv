#pragma once

#include "net/metrics.h"
#include "raft/types.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace forgekv::cluster {

enum class RequestOperation : std::uint8_t { put, get, delete_key, ping };
enum class RequestOutcome : std::uint8_t {
  ok,
  not_found,
  invalid,
  redirect,
  busy,
  internal,
  request_id_reuse,
  stale_request,
  capacity_exceeded,
};

inline constexpr std::size_t kRequestOperationCount = 4U;
inline constexpr std::size_t kRequestOutcomeCount = 9U;
inline constexpr std::array<std::uint64_t, 12> kLatencyBucketsUs{
    50,   100,   250,   500,   1'000,  2'500,
    5'000, 10'000, 25'000, 50'000, 100'000, 500'000};

[[nodiscard]] constexpr std::size_t request_operation_index(
    const RequestOperation operation) noexcept {
  return static_cast<std::size_t>(operation);
}

[[nodiscard]] constexpr std::size_t request_outcome_index(
    const RequestOutcome outcome) noexcept {
  return static_cast<std::size_t>(outcome);
}

struct HistogramSnapshot final {
  std::array<std::uint64_t, kLatencyBucketsUs.size()> buckets{};
  std::uint64_t count{};
  std::uint64_t sum_microseconds{};
};

class AtomicHistogram final {
 public:
  void observe(std::chrono::microseconds duration) noexcept;
  [[nodiscard]] HistogramSnapshot snapshot() const noexcept;

 private:
  std::array<std::atomic<std::uint64_t>, kLatencyBucketsUs.size()> buckets_{};
  std::atomic<std::uint64_t> count_{};
  std::atomic<std::uint64_t> sum_microseconds_{};
};

struct RaftObservation final {
  raft::Role role{raft::Role::follower};
  raft::Term term{};
  std::optional<raft::NodeId> leader_id;
  raft::LogIndex commit_index{};
  raft::LogIndex last_applied{};
  raft::LogIndex last_log_index{};
  std::uint64_t retained_log_records{};
  std::vector<std::pair<raft::NodeId, raft::LogIndex>> peer_match;
};

struct OperationalMetricsSnapshot final {
  std::array<std::uint64_t, kRequestOperationCount> requests_total{};
  std::array<std::uint64_t, kRequestOutcomeCount> errors_total{};
  std::array<HistogramSnapshot, kRequestOperationCount> request_latency{};
  HistogramSnapshot queueing_latency;
  HistogramSnapshot sync_latency;
  HistogramSnapshot recovery_duration;
  HistogramSnapshot snapshot_duration;
  std::uint64_t requests_in_flight{};
  std::uint64_t queue_depth{};
  std::uint64_t queue_rejected_total{};
  std::uint64_t elections_total{};
  std::uint64_t leadership_changes_total{};
  std::uint64_t append_entries_total{};
  std::uint64_t wal_bytes{};
  std::uint64_t sync_count{};
  RaftObservation raft;
  std::unordered_map<raft::NodeId, std::uint64_t> peer_lag;
};

class OperationalMetrics final {
 public:
  explicit OperationalMetrics(std::vector<raft::NodeId> voters);

  void request_started(RequestOperation operation) noexcept;
  void request_finished(RequestOperation operation,
                        std::chrono::microseconds duration) noexcept;
  void request_outcome(RequestOutcome outcome) noexcept;
  void request_rejected(RequestOperation operation) noexcept;
  void set_queue_depth(std::size_t depth) noexcept;
  void queue_rejected() noexcept;
  void observe_queueing(std::chrono::microseconds duration) noexcept;
  void observe_sync(std::chrono::microseconds duration) noexcept;
  void observe_snapshot(std::chrono::microseconds duration) noexcept;
  void recovery_finished(std::chrono::microseconds duration) noexcept;
  void election_started() noexcept;
  void leadership_changed() noexcept;
  void append_entries() noexcept;
  void add_wal_bytes(std::size_t bytes) noexcept;
  void set_raft(const RaftObservation& observation) noexcept;

  [[nodiscard]] OperationalMetricsSnapshot snapshot() const;

 private:
  std::vector<raft::NodeId> voters_;
  std::array<std::atomic<std::uint64_t>, kRequestOperationCount>
      requests_total_{};
  std::array<std::atomic<std::uint64_t>, kRequestOutcomeCount> errors_total_{};
  std::array<AtomicHistogram, kRequestOperationCount> request_latency_{};
  AtomicHistogram queueing_latency_;
  AtomicHistogram sync_latency_;
  AtomicHistogram recovery_duration_;
  AtomicHistogram snapshot_duration_;
  std::atomic<std::uint64_t> requests_in_flight_{};
  std::atomic<std::uint64_t> queue_depth_{};
  std::atomic<std::uint64_t> queue_rejected_total_{};
  std::atomic<std::uint64_t> elections_total_{};
  std::atomic<std::uint64_t> leadership_changes_total_{};
  std::atomic<std::uint64_t> append_entries_total_{};
  std::atomic<std::uint64_t> wal_bytes_{};
  // Raft status has one publisher. The sequence lets scrapers take a coherent
  // multi-field view without blocking the Raft owner thread.
  std::atomic<std::uint64_t> raft_sequence_{};
  std::atomic<std::uint64_t> role_{};
  std::atomic<std::uint64_t> term_{};
  std::atomic<std::uint64_t> leader_id_{};
  std::atomic<bool> leader_known_{};
  std::atomic<std::uint64_t> commit_index_{};
  std::atomic<std::uint64_t> last_applied_{};
  std::atomic<std::uint64_t> last_log_index_{};
  std::atomic<std::uint64_t> retained_log_records_{};
  std::unique_ptr<std::atomic<std::uint64_t>[]> peer_lag_;
  std::unique_ptr<std::atomic<bool>[]> peer_lag_visible_;
};

struct ProcessMetrics final {
  double cpu_seconds{};
  std::uint64_t rss_bytes{};
  std::uint64_t open_fds{};
  std::uint64_t threads{};
};

[[nodiscard]] std::optional<ProcessMetrics> sample_process_metrics();
[[nodiscard]] std::string render_prometheus(
    const OperationalMetricsSnapshot& metrics,
    const std::optional<ProcessMetrics>& process,
    const net::MetricsSnapshot& client_network,
    const net::MetricsSnapshot& peer_network);

}  // namespace forgekv::cluster
