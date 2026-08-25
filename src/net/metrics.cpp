#include "net/metrics.h"

namespace forgekv::net {
namespace {

constexpr auto kOrder = std::memory_order_relaxed;

}  // namespace

void Metrics::connection_opened() noexcept {
  active_connections_.fetch_add(1U, kOrder);
  accepted_connections_total_.fetch_add(1U, kOrder);
}

void Metrics::connection_closed() noexcept {
  active_connections_.fetch_sub(1U, kOrder);
  closed_connections_total_.fetch_add(1U, kOrder);
}

void Metrics::add_bytes_read(const std::size_t count) noexcept {
  bytes_read_total_.fetch_add(count, kOrder);
}

void Metrics::add_bytes_written(const std::size_t count) noexcept {
  bytes_written_total_.fetch_add(count, kOrder);
}

void Metrics::request_started() noexcept {
  requests_in_flight_.fetch_add(1U, kOrder);
}

void Metrics::request_finished() noexcept {
  requests_in_flight_.fetch_sub(1U, kOrder);
}

void Metrics::add_read_buffer_bytes(const std::size_t count) noexcept {
  read_buffer_bytes_.fetch_add(count, kOrder);
}

void Metrics::remove_read_buffer_bytes(const std::size_t count) noexcept {
  read_buffer_bytes_.fetch_sub(count, kOrder);
}

void Metrics::add_write_buffer_bytes(const std::size_t count) noexcept {
  write_buffer_bytes_.fetch_add(count, kOrder);
}

void Metrics::remove_write_buffer_bytes(const std::size_t count) noexcept {
  write_buffer_bytes_.fetch_sub(count, kOrder);
}

void Metrics::backpressure_started() noexcept {
  connections_backpressured_.fetch_add(1U, kOrder);
}

void Metrics::backpressure_ended() noexcept {
  connections_backpressured_.fetch_sub(1U, kOrder);
}

MetricsSnapshot Metrics::snapshot() const noexcept {
  return MetricsSnapshot{
      .active_connections = active_connections_.load(kOrder),
      .accepted_connections_total = accepted_connections_total_.load(kOrder),
      .closed_connections_total = closed_connections_total_.load(kOrder),
      .bytes_read_total = bytes_read_total_.load(kOrder),
      .bytes_written_total = bytes_written_total_.load(kOrder),
      .requests_in_flight = requests_in_flight_.load(kOrder),
      .read_buffer_bytes = read_buffer_bytes_.load(kOrder),
      .write_buffer_bytes = write_buffer_bytes_.load(kOrder),
      .connections_backpressured = connections_backpressured_.load(kOrder),
  };
}

}  // namespace forgekv::net
