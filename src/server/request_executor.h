#pragma once

#include "net/connection_token.h"
#include "protocol/frame.h"

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace forgekv::server {

struct Request {
  net::ConnectionToken token;
  protocol::Frame frame;
  std::size_t wire_bytes{0U};
};

struct Completion {
  net::ConnectionToken token;
  protocol::Frame frame;
};

class RequestExecutor final {
 public:
  using Handler = std::function<protocol::Frame(const protocol::Frame&)>;
  using CompletionSink = std::function<void(Completion)>;

  RequestExecutor(std::size_t worker_count, std::size_t max_items,
                  std::size_t max_bytes, Handler handler,
                  CompletionSink completion_sink);
  ~RequestExecutor();

  RequestExecutor(const RequestExecutor&) = delete;
  RequestExecutor& operator=(const RequestExecutor&) = delete;

  [[nodiscard]] bool try_submit(Request&& request);
  void stop();

 private:
  void worker_loop();
  [[nodiscard]] protocol::Frame error_response(const protocol::Frame& request,
                                               const char* message) const;

  const std::size_t max_items_;
  const std::size_t max_bytes_;
  Handler handler_;
  CompletionSink completion_sink_;

  std::mutex mutex_;
  std::condition_variable condition_;
  std::deque<Request> queue_;
  std::vector<std::thread> workers_;
  std::size_t outstanding_items_{0U};
  std::size_t outstanding_bytes_{0U};
  bool stopping_{false};
};

}  // namespace forgekv::server
