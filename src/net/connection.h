#pragma once

#include "net/connection_token.h"
#include "protocol/parser.h"

#include <chrono>
#include <cstddef>
#include <deque>
#include <vector>

namespace forgekv::net {

struct Connection {
  int fd{-1};
  ConnectionToken token;
  protocol::Parser parser;
  std::deque<std::vector<std::byte>> write_queue;
  std::size_t write_offset{0U};
  std::size_t buffered_output_bytes{0U};
  std::size_t pending_request_count{0U};
  bool read_paused{false};
  bool backpressured{false};
  std::chrono::steady_clock::time_point last_activity{std::chrono::steady_clock::now()};
};

}  // namespace forgekv::net
