#pragma once

#include "protocol/frame.h"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace forgekv::chaos {

using ClientId = std::array<std::byte, 16>;

struct Endpoint final {
  std::string host{"127.0.0.1"};
  std::uint16_t port{};
  bool operator==(const Endpoint&) const = default;
};

enum class ClientOperation : std::uint8_t { put, get, delete_key };

struct LogicalRequest final {
  ClientOperation operation{ClientOperation::get};
  ClientId client_id{};
  std::uint64_t request_id{};
  std::string key;
  std::string value;
  bool operator==(const LogicalRequest&) const = default;
};

enum class AttemptKind : std::uint8_t {
  ok,
  not_found,
  redirect,
  busy,
  server_error,
  timeout,
  transport_error,
  protocol_error,
};

struct AttemptResult final {
  AttemptKind kind{AttemptKind::transport_error};
  std::vector<std::byte> payload;
  std::optional<Endpoint> redirect;
  std::string diagnostic;

  [[nodiscard]] static AttemptResult ok(std::vector<std::byte> payload = {});
  [[nodiscard]] static AttemptResult not_found();
  [[nodiscard]] static AttemptResult timeout();
  [[nodiscard]] static AttemptResult transport_error(std::string diagnostic);
};

struct ObserveResult final {
  bool completed{};
  bool success{};
  std::string diagnostic;
};

class ClientState final {
 public:
  ClientState(ClientId client_id, std::string key);

  [[nodiscard]] LogicalRequest begin_put(std::string value);
  [[nodiscard]] LogicalRequest begin_get();
  [[nodiscard]] LogicalRequest begin_delete();
  [[nodiscard]] std::optional<LogicalRequest> next_attempt() const;
  [[nodiscard]] ObserveResult observe(const LogicalRequest& request,
                                      const AttemptResult& result);

  [[nodiscard]] const ClientId& client_id() const noexcept { return client_id_; }
  [[nodiscard]] const std::string& key() const noexcept { return key_; }
  [[nodiscard]] const std::optional<std::string>& expected_value() const noexcept {
    return expected_value_;
  }

 private:
  [[nodiscard]] LogicalRequest begin(ClientOperation operation,
                                     std::string value);

  ClientId client_id_{};
  std::string key_;
  std::uint64_t next_request_id_{1U};
  std::optional<LogicalRequest> outstanding_;
  std::optional<std::string> expected_value_;
};

[[nodiscard]] protocol::Frame encode_request(const LogicalRequest& request);
[[nodiscard]] std::vector<std::byte> redirect_payload(std::string_view endpoint);
[[nodiscard]] AttemptResult classify_response(const protocol::Frame& response,
                                              std::uint64_t request_id);
[[nodiscard]] AttemptResult execute_attempt(
    const Endpoint& endpoint, const LogicalRequest& request,
    std::chrono::milliseconds timeout = std::chrono::seconds(1));

}  // namespace forgekv::chaos
