#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace forgekv::chaos {

struct LinkPolicy final {
  bool partitioned{};
  std::uint32_t latency_ms{};
  std::uint32_t jitter_ms{};
  std::uint32_t loss_percent{};
  bool operator==(const LinkPolicy&) const = default;
};

struct FaultProxyConfig final {
  std::string bind_host{"127.0.0.1"};
  std::uint16_t bind_port{};
  std::string destination_host{"127.0.0.1"};
  std::uint16_t destination_port{};
  std::uint64_t seed{1U};
  std::size_t maximum_queued_bytes{4U * 1024U * 1024U};
};

struct ProxyStatus final {
  std::string error;
  [[nodiscard]] bool ok() const noexcept { return error.empty(); }
};

class FaultProxy final {
 public:
  explicit FaultProxy(FaultProxyConfig config);
  ~FaultProxy();

  FaultProxy(const FaultProxy&) = delete;
  FaultProxy& operator=(const FaultProxy&) = delete;

  [[nodiscard]] ProxyStatus start();
  [[nodiscard]] ProxyStatus set_policy(LinkPolicy policy);
  [[nodiscard]] LinkPolicy policy() const;
  [[nodiscard]] std::uint16_t port() const noexcept;
  void stop() noexcept;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace forgekv::chaos
