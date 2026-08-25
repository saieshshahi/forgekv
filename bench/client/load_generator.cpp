#include "protocol/parser.h"
#include "protocol/serializer.h"
#include "server/tcp_server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

struct Options {
  std::string host{"127.0.0.1"};
  std::uint16_t port{0U};
  std::size_t concurrency{4U};
  std::size_t pipeline_depth{16U};
  std::size_t requests{10'000U};
  std::size_t key_size{16U};
  std::size_t value_size{100U};
  bool persistent{true};
};

struct WorkerStats {
  std::size_t completed{0U};
  std::size_t errors{0U};
  std::size_t bytes{0U};
  std::vector<double> latency_microseconds;
};

class Socket final {
 public:
  Socket() = default;
  explicit Socket(const int fd) : fd_(fd) {}
  ~Socket() { close(); }
  Socket(const Socket&) = delete;
  Socket& operator=(const Socket&) = delete;
  Socket(Socket&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
  Socket& operator=(Socket&& other) noexcept {
    if (this != &other) {
      close();
      fd_ = std::exchange(other.fd_, -1);
    }
    return *this;
  }
  [[nodiscard]] int get() const noexcept { return fd_; }
  [[nodiscard]] bool valid() const noexcept { return fd_ >= 0; }
  void close() noexcept {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
  }

 private:
  int fd_{-1};
};

template <typename T>
bool parse_integer(const std::string_view text, T& output) {
  const auto* begin = text.data();
  const auto* end = begin + text.size();
  const auto result = std::from_chars(begin, end, output);
  return result.ec == std::errc{} && result.ptr == end;
}

std::optional<std::string> parse_options(const int argc, char** argv, Options& options) {
  for (int index = 1; index < argc; ++index) {
    const std::string_view argument(argv[index]);
    const auto separator = argument.find('=');
    if (!argument.starts_with("--") || separator == std::string_view::npos) {
      return std::string("expected --name=value: ") + std::string(argument);
    }
    const auto name = argument.substr(2U, separator - 2U);
    const auto value = argument.substr(separator + 1U);
    if (name == "host") {
      options.host = value;
    } else if (name == "port") {
      if (!parse_integer(value, options.port)) {
        return "invalid port";
      }
    } else if (name == "concurrency") {
      if (!parse_integer(value, options.concurrency)) {
        return "invalid concurrency";
      }
    } else if (name == "pipeline") {
      if (!parse_integer(value, options.pipeline_depth)) {
        return "invalid pipeline";
      }
    } else if (name == "requests") {
      if (!parse_integer(value, options.requests)) {
        return "invalid requests";
      }
    } else if (name == "key-size") {
      if (!parse_integer(value, options.key_size)) {
        return "invalid key size";
      }
    } else if (name == "value-size") {
      if (!parse_integer(value, options.value_size)) {
        return "invalid value size";
      }
    } else if (name == "persistent") {
      int parsed = 0;
      if (!parse_integer(value, parsed) || (parsed != 0 && parsed != 1)) {
        return "persistent must be 0 or 1";
      }
      options.persistent = parsed == 1;
    } else {
      return std::string("unknown option: ") + std::string(name);
    }
  }
  if (options.concurrency == 0U || options.pipeline_depth == 0U ||
      options.requests == 0U || options.key_size > forgekv::protocol::kMaxKeySize ||
      options.value_size > forgekv::protocol::kMaxValueSize ||
      options.key_size + options.value_size > forgekv::protocol::kMaxPayloadSize) {
    return "counts and sizes are outside supported bounds";
  }
  return std::nullopt;
}

Socket connect_to(const Options& options) {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    return {};
  }
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(options.port);
  if (::inet_pton(AF_INET, options.host.c_str(), &address.sin_addr) != 1 ||
      ::connect(fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
    ::close(fd);
    return {};
  }
  return Socket(fd);
}

bool send_all(const int fd, const std::span<const std::byte> bytes) {
  std::size_t offset = 0U;
  while (offset < bytes.size()) {
    const auto count = ::send(fd, bytes.data() + offset, bytes.size() - offset, MSG_NOSIGNAL);
    if (count <= 0) {
      return false;
    }
    offset += static_cast<std::size_t>(count);
  }
  return true;
}

forgekv::protocol::Frame make_request(const std::uint64_t request_id,
                                      const Options& options) {
  forgekv::protocol::Frame frame{
      .message_namespace = forgekv::protocol::Namespace::client,
      .message_type = forgekv::protocol::MessageType::ping,
      .flags = 0U,
      .request_id = request_id,
      .payload = {},
  };
  frame.payload.resize(options.key_size + options.value_size, std::byte{0x5A});
  return frame;
}

void worker(const std::size_t worker_index, const std::size_t request_count,
            const Options& options, std::barrier<>& start_barrier,
            WorkerStats& stats) {
  Socket socket;
  forgekv::protocol::Parser parser;
  std::vector<std::byte> receive_buffer(64U * 1024U);
  std::size_t issued = 0U;
  start_barrier.arrive_and_wait();

  while (issued < request_count) {
    if (!socket.valid()) {
      socket = connect_to(options);
      if (!socket.valid()) {
        ++stats.errors;
        return;
      }
      parser = forgekv::protocol::Parser{};
    }

    const auto batch_size = std::min(options.pipeline_depth, request_count - issued);
    std::vector<std::byte> output;
    output.reserve(batch_size *
                   (forgekv::protocol::kHeaderSize + options.key_size + options.value_size));
    std::unordered_map<std::uint64_t, Clock::time_point> starts;
    for (std::size_t index = 0U; index < batch_size; ++index) {
      const auto request_id = (static_cast<std::uint64_t>(worker_index) << 48U) |
                              static_cast<std::uint64_t>(issued + index + 1U);
      auto wire = forgekv::protocol::serialize(make_request(request_id, options)).bytes;
      starts.emplace(request_id, Clock::now());
      output.insert(output.end(), wire.begin(), wire.end());
    }
    if (!send_all(socket.get(), output)) {
      ++stats.errors;
      socket.close();
      continue;
    }
    stats.bytes += output.size();

    std::size_t received = 0U;
    while (received < batch_size) {
      const auto count = ::recv(socket.get(), receive_buffer.data(), receive_buffer.size(), 0);
      if (count <= 0) {
        ++stats.errors;
        socket.close();
        break;
      }
      auto parsed = parser.consume(std::span<const std::byte>(receive_buffer).first(
          static_cast<std::size_t>(count)));
      if (!parsed.ok()) {
        ++stats.errors;
        socket.close();
        break;
      }
      stats.bytes += static_cast<std::size_t>(count);
      for (const auto& response : parsed.frames) {
        const auto found = starts.find(response.request_id);
        if (found == starts.end() ||
            response.message_type != forgekv::protocol::MessageType::ok) {
          ++stats.errors;
          continue;
        }
        const auto latency = std::chrono::duration<double, std::micro>(Clock::now() - found->second);
        stats.latency_microseconds.push_back(latency.count());
        ++stats.completed;
        ++received;
      }
    }
    issued += batch_size;
    if (!options.persistent) {
      socket.close();
    }
  }
}

double percentile(const std::vector<double>& values, const double fraction) {
  if (values.empty()) {
    return 0.0;
  }
  const auto raw = std::ceil(fraction * static_cast<double>(values.size())) - 1.0;
  const auto index = static_cast<std::size_t>(std::max(0.0, raw));
  return values[std::min(index, values.size() - 1U)];
}

}  // namespace

int main(const int argc, char** argv) {
  Options options;
  if (const auto error = parse_options(argc, argv, options); error.has_value()) {
    std::cerr << "error: " << *error << '\n';
    return EXIT_FAILURE;
  }

  std::unique_ptr<forgekv::server::TcpServer> local_server;
  if (options.port == 0U) {
    forgekv::net::ReactorConfig config;
    config.worker_threads = std::max<std::size_t>(4U, options.concurrency);
    local_server = std::make_unique<forgekv::server::TcpServer>(
        config, [](const forgekv::protocol::Frame& request) {
          auto response = request;
          response.message_type = forgekv::protocol::MessageType::ok;
          return response;
        });
    if (const auto error = local_server->start(options.host, 0U); error.has_value()) {
      std::cerr << "server start failed: " << *error << '\n';
      return EXIT_FAILURE;
    }
    options.port = local_server->bound_port();
  }

  std::vector<WorkerStats> stats(options.concurrency);
  std::vector<std::thread> threads;
  threads.reserve(options.concurrency);
  std::barrier start_barrier(static_cast<std::ptrdiff_t>(options.concurrency + 1U));
  const auto base = options.requests / options.concurrency;
  const auto remainder = options.requests % options.concurrency;
  for (std::size_t index = 0U; index < options.concurrency; ++index) {
    const auto count = base + (index < remainder ? 1U : 0U);
    threads.emplace_back(worker, index, count, std::cref(options),
                         std::ref(start_barrier), std::ref(stats[index]));
  }

  start_barrier.arrive_and_wait();
  const auto started = Clock::now();
  for (auto& thread : threads) {
    thread.join();
  }
  const auto elapsed = std::chrono::duration<double>(Clock::now() - started).count();

  std::size_t completed = 0U;
  std::size_t errors = 0U;
  std::size_t bytes = 0U;
  std::vector<double> latencies;
  for (auto& worker_stats : stats) {
    completed += worker_stats.completed;
    errors += worker_stats.errors;
    bytes += worker_stats.bytes;
    latencies.insert(latencies.end(), worker_stats.latency_microseconds.begin(),
                     worker_stats.latency_microseconds.end());
  }
  std::ranges::sort(latencies);

  std::cout << "completed=" << completed << " errors=" << errors
            << " ops_per_sec=" << static_cast<double>(completed) / elapsed
            << " bytes_per_sec=" << static_cast<double>(bytes) / elapsed
            << " p50_us=" << percentile(latencies, 0.50)
            << " p95_us=" << percentile(latencies, 0.95)
            << " p99_us=" << percentile(latencies, 0.99)
            << " max_us=" << (latencies.empty() ? 0.0 : latencies.back()) << '\n';

  if (local_server) {
    local_server->stop();
  }
  return errors == 0U && completed == options.requests ? EXIT_SUCCESS : EXIT_FAILURE;
}
