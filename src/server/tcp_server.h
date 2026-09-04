#pragma once

#include "net/metrics.h"
#include "net/reactor_config.h"
#include "server/request_executor.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace forgekv::server {

class TcpServer final {
 public:
  using Handler = RequestExecutor::Handler;
  using RejectionObserver = std::function<void(const RequestMetadata&)>;
  using CompletionObserver =
      std::function<void(const RequestMetadata&, const ResponseMetadata&)>;

  TcpServer(net::ReactorConfig config, Handler handler,
            RejectionObserver rejection_observer = {},
            CompletionObserver completion_observer = {});
  ~TcpServer();

  TcpServer(const TcpServer&) = delete;
  TcpServer& operator=(const TcpServer&) = delete;

  [[nodiscard]] std::optional<std::string> start(std::string_view bind_address,
                                                 std::uint16_t port);
  [[nodiscard]] std::uint16_t bound_port() const noexcept;
  [[nodiscard]] net::MetricsSnapshot metrics() const noexcept;
  void stop();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace forgekv::server
