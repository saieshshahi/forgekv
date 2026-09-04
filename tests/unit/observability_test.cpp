#include "cluster/observability.h"

#include <gtest/gtest.h>

#include <chrono>
#include <atomic>
#include <string>
#include <thread>

namespace forgekv::cluster {
namespace {

using namespace std::chrono_literals;

TEST(OperationalMetricsTest, CapturesFixedCardinalityRequestsAndDurations) {
  OperationalMetrics metrics({1, 2, 3});
  metrics.request_started(RequestOperation::put);
  metrics.request_finished(RequestOperation::put, 75us);
  metrics.request_outcome(RequestOutcome::ok);
  metrics.request_started(RequestOperation::get);
  metrics.request_finished(RequestOperation::get, 3ms);
  metrics.request_outcome(RequestOutcome::busy);
  metrics.request_rejected(RequestOperation::ping);
  metrics.queue_rejected();
  metrics.observe_queueing(250us);
  metrics.observe_sync(2ms);
  metrics.observe_snapshot(12ms);
  metrics.set_raft(RaftObservation{.role = raft::Role::leader,
                                   .term = 7,
                                   .leader_id = 2,
                                   .commit_index = 90,
                                   .last_applied = 89,
                                   .last_log_index = 100,
                                   .retained_log_records = 10,
                                   .peer_match = {{1, 95}, {3, 80}}});
  metrics.election_started();
  metrics.leadership_changed();
  metrics.append_entries();
  metrics.add_wal_bytes(400);
  metrics.recovery_finished(4ms);

  const auto snapshot = metrics.snapshot();
  EXPECT_EQ(snapshot.requests_total[request_operation_index(
                RequestOperation::put)],
            1U);
  EXPECT_EQ(snapshot.errors_total[request_outcome_index(RequestOutcome::busy)],
            2U);
  EXPECT_EQ(snapshot.requests_total[request_operation_index(
                RequestOperation::ping)],
            1U);
  EXPECT_EQ(snapshot.request_latency[request_operation_index(
                RequestOperation::ping)]
                .count,
            0U);
  EXPECT_EQ(snapshot.requests_in_flight, 0U);
  EXPECT_EQ(snapshot.queue_rejected_total, 1U);
  EXPECT_EQ(snapshot.sync_count, 1U);
  EXPECT_EQ(snapshot.elections_total, 1U);
  EXPECT_EQ(snapshot.leadership_changes_total, 1U);
  EXPECT_EQ(snapshot.append_entries_total, 1U);
  EXPECT_EQ(snapshot.raft.term, 7U);
  EXPECT_EQ(snapshot.peer_lag.at(1), 5U);
  EXPECT_EQ(snapshot.peer_lag.at(3), 20U);
}

TEST(OperationalMetricsTest, FinalOutcomeIsRecordedAfterHandlerCompletion) {
  OperationalMetrics metrics({1, 2, 3});
  metrics.request_started(RequestOperation::put);
  metrics.request_finished(RequestOperation::put, 75us);

  const auto before_outcome = metrics.snapshot();
  EXPECT_EQ(before_outcome.requests_total[request_operation_index(
                RequestOperation::put)],
            1U);
  EXPECT_EQ(before_outcome.errors_total[request_outcome_index(
                RequestOutcome::invalid)],
            0U);
  EXPECT_EQ(before_outcome.errors_total[request_outcome_index(
                RequestOutcome::busy)],
            0U);

  metrics.request_outcome(RequestOutcome::busy);

  const auto snapshot = metrics.snapshot();
  EXPECT_EQ(snapshot.requests_total[request_operation_index(
                RequestOperation::put)],
            1U);
  EXPECT_EQ(snapshot.errors_total[request_outcome_index(RequestOutcome::busy)],
            1U);
  EXPECT_EQ(snapshot.request_latency[request_operation_index(
                RequestOperation::put)]
                .count,
            1U);
}

TEST(OperationalMetricsTest, RendersPrometheusWithoutUnboundedLabels) {
  OperationalMetrics metrics({1, 2, 3});
  metrics.request_started(RequestOperation::delete_key);
  metrics.request_finished(RequestOperation::delete_key, 700us);
  metrics.request_outcome(RequestOutcome::request_id_reuse);
  metrics.set_raft(RaftObservation{.role = raft::Role::follower,
                                   .term = 11,
                                   .leader_id = 3,
                                   .commit_index = 40,
                                   .last_applied = 40,
                                   .last_log_index = 42,
                                   .retained_log_records = 2,
                                   .peer_match = {}});
  const ProcessMetrics process{.cpu_seconds = 1.25,
                               .rss_bytes = 4096,
                               .open_fds = 8,
                               .threads = 4};
  const net::MetricsSnapshot client_network{
      .active_connections = 2,
      .accepted_connections_total = 9,
      .closed_connections_total = 7,
      .bytes_read_total = 100,
      .bytes_written_total = 80,
      .requests_in_flight = 1,
      .read_buffer_bytes = 5,
      .write_buffer_bytes = 6,
      .connections_backpressured = 1,
      .backpressure_events_total = 4,
      .rejected_requests_total = 3};
  const auto text = render_prometheus(metrics.snapshot(), process,
                                      client_network, {});

  EXPECT_NE(text.find("forgekv_process_rss_bytes 4096"), std::string::npos);
  EXPECT_NE(text.find("forgekv_requests_total{op=\"delete\"} 1"),
            std::string::npos);
  EXPECT_NE(text.find(
                "forgekv_errors_total{type=\"request_id_reuse\"} 1"),
            std::string::npos);
  EXPECT_NE(text.find("forgekv_raft_role{role=\"follower\"} 1"),
            std::string::npos);
  EXPECT_NE(text.find("forgekv_raft_leader_id 3"), std::string::npos);
  EXPECT_NE(text.find("forgekv_network_rx_bytes_total 100"),
            std::string::npos);
  EXPECT_NE(text.find("forgekv_network_rejected_requests_total 3"),
            std::string::npos);
  EXPECT_NE(text.find("forgekv_queue_rejected_total 3"), std::string::npos);
  EXPECT_EQ(text.find("client_id="), std::string::npos);
  EXPECT_EQ(text.find("key="), std::string::npos);
}

TEST(OperationalMetricsTest, ConcurrentHistogramSnapshotsRemainStructural) {
  AtomicHistogram histogram;
  std::jthread writer([&](const std::stop_token stop) {
    while (!stop.stop_requested()) {
      histogram.observe(75us);
    }
  });

  for (std::size_t iteration = 0; iteration < 10'000U; ++iteration) {
    const auto snapshot = histogram.snapshot();
    std::uint64_t finite_count = 0;
    for (const auto count : snapshot.buckets) {
      finite_count += count;
    }
    ASSERT_LE(finite_count, snapshot.count);
  }
  writer.request_stop();
}

TEST(OperationalMetricsTest, ConcurrentRaftSnapshotsRemainCoherent) {
  OperationalMetrics metrics({1, 2, 3});
  const RaftObservation follower{
      .role = raft::Role::follower,
      .term = 11,
      .leader_id = 2,
      .commit_index = 101,
      .last_applied = 100,
      .last_log_index = 103,
      .retained_log_records = 3,
      .peer_match = {},
  };
  const RaftObservation leader{
      .role = raft::Role::leader,
      .term = 12,
      .leader_id = 1,
      .commit_index = 202,
      .last_applied = 201,
      .last_log_index = 207,
      .retained_log_records = 5,
      .peer_match = {{1, 207}, {2, 205}, {3, 200}},
  };
  metrics.set_raft(follower);
  std::jthread writer([&](const std::stop_token stop) {
    while (!stop.stop_requested()) {
      metrics.set_raft(leader);
      metrics.set_raft(follower);
      std::this_thread::yield();
    }
  });

  for (std::size_t iteration = 0; iteration < 25'000U; ++iteration) {
    const auto snapshot = metrics.snapshot();
    if (snapshot.raft.role == raft::Role::leader) {
      ASSERT_EQ(snapshot.raft.term, 12U);
      ASSERT_EQ(snapshot.raft.leader_id, 1U);
      ASSERT_EQ(snapshot.raft.commit_index, 202U);
      ASSERT_EQ(snapshot.raft.last_applied, 201U);
      ASSERT_EQ(snapshot.raft.last_log_index, 207U);
      ASSERT_EQ(snapshot.raft.retained_log_records, 5U);
      ASSERT_EQ(snapshot.peer_lag.size(), 3U);
      ASSERT_EQ(snapshot.peer_lag.at(2), 2U);
      ASSERT_EQ(snapshot.peer_lag.at(3), 7U);
    } else {
      ASSERT_EQ(snapshot.raft.role, raft::Role::follower);
      ASSERT_EQ(snapshot.raft.term, 11U);
      ASSERT_EQ(snapshot.raft.leader_id, 2U);
      ASSERT_EQ(snapshot.raft.commit_index, 101U);
      ASSERT_EQ(snapshot.raft.last_applied, 100U);
      ASSERT_EQ(snapshot.raft.last_log_index, 103U);
      ASSERT_EQ(snapshot.raft.retained_log_records, 3U);
      ASSERT_TRUE(snapshot.peer_lag.empty());
    }
  }
  writer.request_stop();
}

TEST(ProcessMetricsTest, SamplesTheCurrentLinuxProcess) {
  const auto process = sample_process_metrics();
  ASSERT_TRUE(process.has_value());
  EXPECT_GT(process->rss_bytes, 0U);
  EXPECT_GT(process->open_fds, 0U);
  EXPECT_GT(process->threads, 0U);
}

}  // namespace
}  // namespace forgekv::cluster
