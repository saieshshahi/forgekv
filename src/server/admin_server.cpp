#include "server/admin_server.h"

#include "net/socket_ops.h"

#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

namespace forgekv::server {
namespace {

constexpr std::size_t kMaxRequestBytes = 4096U;

class DescriptorGuard final {
 public:
  explicit DescriptorGuard(const int descriptor) noexcept
      : descriptor_(descriptor) {}
  ~DescriptorGuard() {
    if (descriptor_ >= 0) {
      static_cast<void>(::close(descriptor_));
    }
  }

  DescriptorGuard(const DescriptorGuard&) = delete;
  DescriptorGuard& operator=(const DescriptorGuard&) = delete;

  void release() noexcept { descriptor_ = -1; }

 private:
  int descriptor_;
};

std::string_view reason(const int status) {
  switch (status) {
    case 200:
      return "OK";
    case 400:
      return "Bad Request";
    case 404:
      return "Not Found";
    case 405:
      return "Method Not Allowed";
    case 503:
      return "Service Unavailable";
    default:
      return "Internal Server Error";
  }
}

bool send_all(const int descriptor, const std::string_view bytes) {
  std::size_t offset = 0;
  while (offset < bytes.size()) {
    const auto count = ::send(descriptor, bytes.data() + offset,
                              bytes.size() - offset, MSG_NOSIGNAL);
    if (count > 0) {
      offset += static_cast<std::size_t>(count);
    } else if (count < 0 && errno == EINTR) {
      continue;
    } else {
      return false;
    }
  }
  return true;
}

void send_response(const int descriptor, const AdminResponse& response) {
  const auto status = response.status == 200 || response.status == 400 ||
                              response.status == 404 || response.status == 405 ||
                              response.status == 503
                          ? response.status
                          : 500;
  std::string header = "HTTP/1.1 " + std::to_string(status) + " " +
                       std::string(reason(status)) + "\r\nContent-Type: " +
                       response.content_type + "\r\nContent-Length: " +
                       std::to_string(response.body.size()) +
                       "\r\nConnection: close\r\nCache-Control: no-store\r\n\r\n";
  static_cast<void>(send_all(descriptor, header));
  static_cast<void>(send_all(descriptor, response.body));
}

}  // namespace

class AdminServer::Impl final {
 public:
  explicit Impl(Handler handler) : handler_(std::move(handler)) {
    if (!handler_) {
      throw std::invalid_argument("admin server requires a handler");
    }
  }

  ~Impl() { stop(); }

  std::optional<std::string> start(const std::string_view bind_address,
                                   const std::uint16_t port) {
    const std::lock_guard lock(lifecycle_mutex_);
    if (thread_.joinable() || listener_.load() >= 0) {
      return "admin server can be started only once";
    }
    auto listener = net::create_listener(bind_address, port);
    if (!listener.ok()) {
      return listener.error;
    }
    DescriptorGuard listener_guard(listener.fd);
    listener_.store(listener.fd, std::memory_order_release);
    bound_port_.store(listener.port, std::memory_order_release);
    stopping_.store(false, std::memory_order_release);
    try {
      thread_ = std::thread([this] { run(); });
    } catch (...) {
      listener_.store(-1, std::memory_order_release);
      bound_port_.store(0U, std::memory_order_release);
      stopping_.store(true, std::memory_order_release);
      throw;
    }
    listener_guard.release();
    return std::nullopt;
  }

  std::uint16_t bound_port() const noexcept {
    return bound_port_.load(std::memory_order_acquire);
  }

  void stop() {
    std::thread thread;
    {
      const std::lock_guard lock(lifecycle_mutex_);
      stopping_.store(true, std::memory_order_release);
      thread = std::move(thread_);
    }
    if (thread.joinable()) {
      thread.join();
    }
  }

 private:
  void run() {
    while (!stopping_.load(std::memory_order_acquire)) {
      const auto listener = listener_.load(std::memory_order_acquire);
      if (listener < 0) {
        return;
      }
      pollfd event{.fd = listener, .events = POLLIN, .revents = 0};
      const auto ready = ::poll(&event, 1, 100);
      if (ready < 0 && errno == EINTR) {
        continue;
      }
      if (ready <= 0 || (event.revents & POLLIN) == 0) {
        continue;
      }
      const int descriptor =
          ::accept4(listener, nullptr, nullptr, SOCK_CLOEXEC);
      if (descriptor < 0) {
        continue;
      }
      handle(descriptor);
      static_cast<void>(::close(descriptor));
    }
    const auto listener = listener_.exchange(-1, std::memory_order_acq_rel);
    if (listener >= 0) {
      static_cast<void>(::close(listener));
    }
  }

  void handle(const int descriptor) {
    timeval timeout{.tv_sec = 1, .tv_usec = 0};
    static_cast<void>(::setsockopt(descriptor, SOL_SOCKET, SO_RCVTIMEO,
                                   &timeout, sizeof(timeout)));
    static_cast<void>(::setsockopt(descriptor, SOL_SOCKET, SO_SNDTIMEO,
                                   &timeout, sizeof(timeout)));
    std::string request;
    std::array<char, 1024> bytes{};
    while (request.find("\r\n\r\n") == std::string::npos) {
      const auto count = ::recv(descriptor, bytes.data(), bytes.size(), 0);
      if (count <= 0) {
        send_response(descriptor, AdminResponse{.status = 400,
                                                .body = "bad request\n"});
        return;
      }
      request.append(bytes.data(), static_cast<std::size_t>(count));
      if (request.size() > kMaxRequestBytes) {
        send_response(descriptor, AdminResponse{.status = 400,
                                                .body = "bad request\n"});
        return;
      }
    }

    const auto line_end = request.find("\r\n");
    const auto first_space = request.find(' ');
    const auto second_space = first_space == std::string::npos
                                  ? std::string::npos
                                  : request.find(' ', first_space + 1U);
    if (line_end == std::string::npos || first_space == std::string::npos ||
        second_space == std::string::npos || second_space > line_end) {
      send_response(descriptor,
                    AdminResponse{.status = 400, .body = "bad request\n"});
      return;
    }
    const auto method = std::string_view(request).substr(0, first_space);
    if (method != "GET") {
      send_response(descriptor, AdminResponse{.status = 405,
                                              .body = "method not allowed\n"});
      return;
    }
    const auto path = std::string_view(request).substr(
        first_space + 1U, second_space - first_space - 1U);
    const auto version = std::string_view(request).substr(
        second_space + 1U, line_end - second_space - 1U);
    if (version != "HTTP/1.0" && version != "HTTP/1.1") {
      send_response(descriptor,
                    AdminResponse{.status = 400, .body = "bad request\n"});
      return;
    }
    try {
      send_response(descriptor, handler_(path));
    } catch (...) {
      send_response(descriptor, AdminResponse{.status = 500,
                                              .body = "internal error\n"});
    }
  }

  Handler handler_;
  std::mutex lifecycle_mutex_;
  std::thread thread_;
  std::atomic<int> listener_{-1};
  std::atomic<std::uint16_t> bound_port_{};
  std::atomic<bool> stopping_{};
};

AdminServer::AdminServer(Handler handler)
    : impl_(std::make_unique<Impl>(std::move(handler))) {}
AdminServer::~AdminServer() = default;

std::optional<std::string> AdminServer::start(
    const std::string_view bind_address, const std::uint16_t port) {
  return impl_->start(bind_address, port);
}

std::uint16_t AdminServer::bound_port() const noexcept {
  return impl_->bound_port();
}

void AdminServer::stop() { impl_->stop(); }

}  // namespace forgekv::server
