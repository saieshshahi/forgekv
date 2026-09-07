#include "chaos/client_worker.h"

#include "protocol/parser.h"
#include "protocol/serializer.h"
#include "protocol/wire.h"

#include <algorithm>
#include <arpa/inet.h>
#include <cerrno>
#include <charconv>
#include <limits>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>

namespace forgekv::chaos {
namespace {

void copy_text(const std::string_view text, std::span<std::byte> destination) {
  for (std::size_t index = 0U; index < text.size(); ++index) {
    destination[index] =
        static_cast<std::byte>(static_cast<unsigned char>(text[index]));
  }
}

std::optional<Endpoint> decode_redirect(
    const std::vector<std::byte>& payload) {
  if (payload.size() < 2U) {
    return std::nullopt;
  }
  const auto size = static_cast<std::size_t>(
      (std::to_integer<std::uint16_t>(payload[0]) << 8U) |
      std::to_integer<std::uint16_t>(payload[1]));
  if (size == 0U || payload.size() != size + 2U) {
    return std::nullopt;
  }
  const std::string text(
      reinterpret_cast<const char*>(payload.data() + 2U), size);
  const auto separator = text.rfind(':');
  if (separator == std::string::npos || separator == 0U ||
      separator + 1U == text.size()) {
    return std::nullopt;
  }
  const auto host = text.substr(0U, separator);
  if (host != "127.0.0.1") {
    return std::nullopt;
  }
  std::uint32_t port = 0U;
  const auto port_text = std::string_view(text).substr(separator + 1U);
  const auto [end, error] = std::from_chars(port_text.data(),
                                            port_text.data() + port_text.size(),
                                            port);
  if (error != std::errc{} || end != port_text.data() + port_text.size() ||
      port == 0U || port > std::numeric_limits<std::uint16_t>::max()) {
    return std::nullopt;
  }
  return Endpoint{.host = host, .port = static_cast<std::uint16_t>(port)};
}

class Socket final {
 public:
  explicit Socket(const int descriptor) : descriptor_(descriptor) {}
  ~Socket() {
    if (descriptor_ >= 0) {
      static_cast<void>(::close(descriptor_));
    }
  }
  Socket(const Socket&) = delete;
  Socket& operator=(const Socket&) = delete;
  [[nodiscard]] int get() const noexcept { return descriptor_; }

 private:
  int descriptor_;
};

bool send_all(const int descriptor, const std::vector<std::byte>& bytes) {
  std::size_t sent = 0U;
  while (sent < bytes.size()) {
    const auto count =
        ::send(descriptor, bytes.data() + sent, bytes.size() - sent, MSG_NOSIGNAL);
    if (count < 0 && errno == EINTR) {
      continue;
    }
    if (count <= 0) {
      return false;
    }
    sent += static_cast<std::size_t>(count);
  }
  return true;
}

}  // namespace

AttemptResult AttemptResult::ok(std::vector<std::byte> payload) {
  return AttemptResult{.kind = AttemptKind::ok, .payload = std::move(payload)};
}

AttemptResult AttemptResult::not_found() {
  return AttemptResult{.kind = AttemptKind::not_found};
}

AttemptResult AttemptResult::timeout() {
  return AttemptResult{.kind = AttemptKind::timeout,
                       .diagnostic = "request timed out"};
}

AttemptResult AttemptResult::transport_error(std::string diagnostic) {
  return AttemptResult{.kind = AttemptKind::transport_error,
                       .diagnostic = std::move(diagnostic)};
}

ClientState::ClientState(ClientId client_id, std::string key)
    : client_id_(client_id), key_(std::move(key)) {
  if (key_.empty() || key_.size() > protocol::kMaxKeySize) {
    throw std::invalid_argument("invalid chaos client key");
  }
}

LogicalRequest ClientState::begin(const ClientOperation operation,
                                  std::string value) {
  if (outstanding_.has_value()) {
    throw std::logic_error("client already has an outstanding request");
  }
  if (next_request_id_ == 0U) {
    throw std::overflow_error("client request ID exhausted");
  }
  outstanding_ = LogicalRequest{.operation = operation,
                                .client_id = client_id_,
                                .request_id = next_request_id_,
                                .key = key_,
                                .value = std::move(value)};
  return *outstanding_;
}

LogicalRequest ClientState::begin_put(std::string value) {
  if (value.size() > protocol::kMaxValueSize) {
    throw std::invalid_argument("chaos client value is too large");
  }
  return begin(ClientOperation::put, std::move(value));
}

LogicalRequest ClientState::begin_get() {
  return begin(ClientOperation::get, {});
}

LogicalRequest ClientState::begin_delete() {
  return begin(ClientOperation::delete_key, {});
}

std::optional<LogicalRequest> ClientState::next_attempt() const {
  return outstanding_;
}

ObserveResult ClientState::observe(const LogicalRequest& request,
                                   const AttemptResult& result) {
  if (!outstanding_.has_value() || request != *outstanding_) {
    throw std::logic_error("attempt does not match outstanding request");
  }
  switch (result.kind) {
    case AttemptKind::timeout:
    case AttemptKind::transport_error:
    case AttemptKind::protocol_error:
    case AttemptKind::redirect:
    case AttemptKind::busy:
      return {.completed = false,
              .success = false,
              .diagnostic = result.diagnostic};
    case AttemptKind::server_error:
      outstanding_.reset();
      ++next_request_id_;
      return {.completed = true,
              .success = false,
              .diagnostic = result.diagnostic};
    case AttemptKind::ok: {
      bool success = true;
      std::string diagnostic;
      if (request.operation == ClientOperation::put) {
        expected_value_ = request.value;
      } else if (request.operation == ClientOperation::delete_key) {
        expected_value_.reset();
      } else {
        const std::string value(
            reinterpret_cast<const char*>(result.payload.data()),
            result.payload.size());
        success = expected_value_.has_value() && value == *expected_value_;
        if (!success) {
          diagnostic = "GET result differs from acknowledged state";
        }
      }
      outstanding_.reset();
      ++next_request_id_;
      return {.completed = true,
              .success = success,
              .diagnostic = std::move(diagnostic)};
    }
    case AttemptKind::not_found: {
      bool success = true;
      if (request.operation == ClientOperation::delete_key) {
        expected_value_.reset();
      } else if (request.operation == ClientOperation::get) {
        success = !expected_value_.has_value();
      } else {
        success = false;
      }
      outstanding_.reset();
      ++next_request_id_;
      return {.completed = true,
              .success = success,
              .diagnostic = success ? std::string{}
                                    : "unexpected not-found response"};
    }
  }
  return {.diagnostic = "unknown response"};
}

protocol::Frame encode_request(const LogicalRequest& request) {
  if (request.request_id == 0U || request.key.empty() ||
      request.key.size() > protocol::kMaxKeySize ||
      request.value.size() > protocol::kMaxValueSize) {
    throw std::invalid_argument("invalid logical request");
  }
  protocol::Frame frame{.message_namespace = protocol::Namespace::client,
                        .message_type = protocol::MessageType::get,
                        .flags = 0U,
                        .request_id = request.request_id};
  if (request.operation == ClientOperation::get) {
    frame.payload.resize(4U + request.key.size());
    protocol::wire::write_u32(std::span{frame.payload}.first<4U>(),
                              static_cast<std::uint32_t>(request.key.size()));
    copy_text(request.key, std::span{frame.payload}.subspan(4U));
    return frame;
  }
  const bool put = request.operation == ClientOperation::put;
  frame.message_type = put ? protocol::MessageType::put
                           : protocol::MessageType::delete_key;
  const std::size_t header = put ? 24U : 20U;
  frame.payload.resize(header + request.key.size() + request.value.size());
  std::ranges::copy(request.client_id, frame.payload.begin());
  protocol::wire::write_u32(std::span{frame.payload}.subspan(16U, 4U),
                            static_cast<std::uint32_t>(request.key.size()));
  if (put) {
    protocol::wire::write_u32(std::span{frame.payload}.subspan(20U, 4U),
                              static_cast<std::uint32_t>(request.value.size()));
  }
  copy_text(request.key, std::span{frame.payload}.subspan(header,
                                                          request.key.size()));
  copy_text(request.value,
            std::span{frame.payload}.subspan(header + request.key.size()));
  return frame;
}

std::vector<std::byte> redirect_payload(const std::string_view endpoint) {
  if (endpoint.empty() || endpoint.size() >
                              std::numeric_limits<std::uint16_t>::max()) {
    throw std::invalid_argument("invalid redirect endpoint");
  }
  std::vector<std::byte> result(2U + endpoint.size());
  const auto size = static_cast<std::uint16_t>(endpoint.size());
  result[0] = static_cast<std::byte>((size >> 8U) & 0xFFU);
  result[1] = static_cast<std::byte>(size & 0xFFU);
  copy_text(endpoint, std::span{result}.subspan(2U));
  return result;
}

AttemptResult classify_response(const protocol::Frame& response,
                                const std::uint64_t request_id) {
  if (response.message_namespace != protocol::Namespace::client ||
      response.request_id != request_id) {
    return AttemptResult{.kind = AttemptKind::protocol_error,
                         .diagnostic = "response identity mismatch"};
  }
  switch (response.message_type) {
    case protocol::MessageType::ok:
      return AttemptResult::ok(response.payload);
    case protocol::MessageType::not_found:
      return AttemptResult::not_found();
    case protocol::MessageType::busy:
      return AttemptResult{.kind = AttemptKind::busy,
                           .diagnostic = "server busy"};
    case protocol::MessageType::error:
      return AttemptResult{.kind = AttemptKind::server_error,
                           .payload = response.payload,
                           .diagnostic = "server error"};
    case protocol::MessageType::redirect: {
      auto endpoint = decode_redirect(response.payload);
      if (!endpoint.has_value()) {
        return AttemptResult{.kind = AttemptKind::protocol_error,
                             .diagnostic = "invalid redirect endpoint"};
      }
      return AttemptResult{.kind = AttemptKind::redirect,
                           .redirect = std::move(endpoint)};
    }
    default:
      return AttemptResult{.kind = AttemptKind::protocol_error,
                           .diagnostic = "unexpected response type"};
  }
}

AttemptResult execute_attempt(const Endpoint& endpoint,
                              const LogicalRequest& request,
                              const std::chrono::milliseconds timeout) {
  if (endpoint.host != "127.0.0.1" || endpoint.port == 0U ||
      timeout.count() <= 0) {
    return AttemptResult{.kind = AttemptKind::protocol_error,
                         .diagnostic = "invalid request endpoint"};
  }
  Socket socket(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0));
  if (socket.get() < 0) {
    return AttemptResult::transport_error("socket failed");
  }
  const auto seconds = timeout.count() / 1000;
  const auto micros = (timeout.count() % 1000) * 1000;
  timeval time{.tv_sec = static_cast<time_t>(seconds),
               .tv_usec = static_cast<suseconds_t>(micros)};
  static_cast<void>(::setsockopt(socket.get(), SOL_SOCKET, SO_RCVTIMEO, &time,
                                 sizeof(time)));
  static_cast<void>(::setsockopt(socket.get(), SOL_SOCKET, SO_SNDTIMEO, &time,
                                 sizeof(time)));
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(endpoint.port);
  if (::connect(socket.get(), reinterpret_cast<sockaddr*>(&address),
                sizeof(address)) != 0) {
    return AttemptResult::transport_error("connect failed");
  }
  const auto encoded = protocol::serialize(encode_request(request));
  if (!encoded.ok() || !send_all(socket.get(), encoded.bytes)) {
    return AttemptResult::transport_error("send failed");
  }
  protocol::Parser parser;
  std::array<std::byte, 4096U> bytes{};
  while (true) {
    const auto count = ::recv(socket.get(), bytes.data(), bytes.size(), 0);
    if (count < 0 && errno == EINTR) {
      continue;
    }
    if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      return AttemptResult::timeout();
    }
    if (count <= 0) {
      return AttemptResult::transport_error("connection closed");
    }
    auto parsed = parser.consume(
        std::span<const std::byte>{bytes}.first(static_cast<std::size_t>(count)));
    if (!parsed.ok() || parsed.frames.size() > 1U) {
      return AttemptResult{.kind = AttemptKind::protocol_error,
                           .diagnostic = "invalid response frame"};
    }
    if (!parsed.frames.empty()) {
      return classify_response(parsed.frames.front(), request.request_id);
    }
  }
}

}  // namespace forgekv::chaos
