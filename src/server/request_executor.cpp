#include "server/request_executor.h"

#include <algorithm>
#include <cstddef>
#include <exception>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace forgekv::server {

RequestExecutor::RequestExecutor(const std::size_t worker_count,
                                 const std::size_t max_items,
                                 const std::size_t max_bytes, Handler handler,
                                 CompletionSink completion_sink)
    : max_items_(max_items),
      max_bytes_(max_bytes),
      handler_(std::move(handler)),
      completion_sink_(std::move(completion_sink)) {
  if (worker_count == 0U || max_items == 0U || max_bytes == 0U || !handler_ ||
      !completion_sink_) {
    throw std::invalid_argument("request executor requires positive limits and callbacks");
  }
  workers_.reserve(worker_count);
  for (std::size_t index = 0U; index < worker_count; ++index) {
    workers_.emplace_back([this] { worker_loop(); });
  }
}

RequestExecutor::~RequestExecutor() {
  stop();
}

bool RequestExecutor::try_submit(Request&& request) {
  const std::lock_guard lock(mutex_);
  if (stopping_ || outstanding_items_ >= max_items_ ||
      request.wire_bytes > max_bytes_ - outstanding_bytes_) {
    return false;
  }

  ++outstanding_items_;
  outstanding_bytes_ += request.wire_bytes;
  queue_.push_back(std::move(request));
  condition_.notify_one();
  return true;
}

void RequestExecutor::stop() {
  {
    const std::lock_guard lock(mutex_);
    if (stopping_ && workers_.empty()) {
      return;
    }
    stopping_ = true;
  }
  condition_.notify_all();

  for (auto& worker : workers_) {
    if (worker.joinable()) {
      worker.join();
    }
  }
  workers_.clear();
}

void RequestExecutor::worker_loop() {
  while (true) {
    Request request;
    {
      std::unique_lock lock(mutex_);
      condition_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
      if (queue_.empty()) {
        return;
      }
      request = std::move(queue_.front());
      queue_.pop_front();
    }

    protocol::Frame response;
    try {
      response = handler_(request.frame);
    } catch (const std::exception& exception) {
      response = error_response(request.frame, exception.what());
    } catch (...) {
      response = error_response(request.frame, "unknown handler failure");
    }

    {
      const std::lock_guard lock(mutex_);
      --outstanding_items_;
      outstanding_bytes_ -= request.wire_bytes;
    }

    try {
      completion_sink_(Completion{.token = request.token, .frame = std::move(response)});
    } catch (...) {
      // Completion delivery is best-effort during shutdown. The worker must stay alive.
    }
  }
}

protocol::Frame RequestExecutor::error_response(const protocol::Frame& request,
                                                const char* message) const {
  constexpr std::size_t kMaximumDiagnosticBytes = 1024U;
  const auto text = std::string_view(message == nullptr ? "handler failure" : message);
  const auto count = std::min(text.size(), kMaximumDiagnosticBytes);
  const auto* begin = reinterpret_cast<const std::byte*>(text.data());
  return protocol::Frame{
      .message_namespace = protocol::Namespace::client,
      .message_type = protocol::MessageType::error,
      .flags = 0U,
      .request_id = request.request_id,
      .payload = std::vector<std::byte>(begin, begin + count),
  };
}

}  // namespace forgekv::server
