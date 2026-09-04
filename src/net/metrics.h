#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace forgekv::net {

struct MetricsSnapshot {
  std::uint64_t active_connections{0U};
  std::uint64_t accepted_connections_total{0U};
  std::uint64_t closed_connections_total{0U};
  std::uint64_t bytes_read_total{0U};
  std::uint64_t bytes_written_total{0U};
  std::uint64_t requests_in_flight{0U};
  std::uint64_t read_buffer_bytes{0U};
  std::uint64_t write_buffer_bytes{0U};
  std::uint64_t connections_backpressured{0U};
  std::uint64_t backpressure_events_total{0U};
  std::uint64_t rejected_requests_total{0U};
};

class Metrics final {
 public:
  void connection_opened() noexcept;
  void connection_closed() noexcept;
  void add_bytes_read(std::size_t count) noexcept;
  void add_bytes_written(std::size_t count) noexcept;
  void request_started() noexcept;
  void request_finished() noexcept;
  void add_read_buffer_bytes(std::size_t count) noexcept;
  void remove_read_buffer_bytes(std::size_t count) noexcept;
  void add_write_buffer_bytes(std::size_t count) noexcept;
  void remove_write_buffer_bytes(std::size_t count) noexcept;
  void backpressure_started() noexcept;
  void backpressure_ended() noexcept;
  void request_rejected() noexcept;

  [[nodiscard]] MetricsSnapshot snapshot() const noexcept;

 private:
  std::atomic<std::uint64_t> active_connections_{0U};
  std::atomic<std::uint64_t> accepted_connections_total_{0U};
  std::atomic<std::uint64_t> closed_connections_total_{0U};
  std::atomic<std::uint64_t> bytes_read_total_{0U};
  std::atomic<std::uint64_t> bytes_written_total_{0U};
  std::atomic<std::uint64_t> requests_in_flight_{0U};
  std::atomic<std::uint64_t> read_buffer_bytes_{0U};
  std::atomic<std::uint64_t> write_buffer_bytes_{0U};
  std::atomic<std::uint64_t> connections_backpressured_{0U};
  std::atomic<std::uint64_t> backpressure_events_total_{0U};
  std::atomic<std::uint64_t> rejected_requests_total_{0U};
};

}  // namespace forgekv::net
