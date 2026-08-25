#pragma once

#include "protocol/frame.h"

#include <cstddef>
#include <optional>
#include <string>

namespace forgekv::net {

struct ReactorConfig {
  std::size_t max_request_size{protocol::kMaxFrameSize};
  std::size_t max_buffered_input_per_connection{protocol::kMaxFrameSize};
  std::size_t max_buffered_output_per_connection{4U * protocol::kMaxFrameSize};
  std::size_t max_requests_in_flight_per_connection{64U};
  std::size_t global_outstanding_work{4096U};
  std::size_t global_queued_work_bytes{64U * 1024U * 1024U};
  std::size_t max_connections{10'000U};
  std::size_t max_events_per_wait{256U};
  std::size_t read_budget_per_event{256U * 1024U};
  std::size_t write_budget_per_event{256U * 1024U};
  std::size_t worker_threads{4U};

  [[nodiscard]] std::optional<std::string> validation_error() const {
    if (max_request_size < protocol::kHeaderSize ||
        max_request_size > protocol::kMaxFrameSize) {
      return "max_request_size is outside protocol bounds";
    }
    if (max_buffered_input_per_connection < max_request_size) {
      return "input buffer limit is smaller than one maximum request";
    }
    if (max_buffered_output_per_connection < protocol::kHeaderSize) {
      return "output buffer limit is smaller than one frame header";
    }
    if (max_requests_in_flight_per_connection == 0U) {
      return "per-connection in-flight limit must be positive";
    }
    if (global_outstanding_work < max_requests_in_flight_per_connection) {
      return "global work limit is smaller than the per-connection limit";
    }
    if (global_queued_work_bytes < max_request_size) {
      return "global work byte limit is smaller than one maximum request";
    }
    if (max_connections == 0U || max_events_per_wait == 0U ||
        read_budget_per_event == 0U || write_budget_per_event == 0U ||
        worker_threads == 0U) {
      return "reactor counts, budgets, and worker count must be positive";
    }
    return std::nullopt;
  }
};

}  // namespace forgekv::net
