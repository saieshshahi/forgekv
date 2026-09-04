#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace forgekv::server {

struct AdminResponse final {
  int status{500};
  std::string content_type{"text/plain; charset=utf-8"};
  std::string body;
};

class AdminServer final {
 public:
  using Handler = std::function<AdminResponse(std::string_view)>;

  explicit AdminServer(Handler handler);
  ~AdminServer();

  AdminServer(const AdminServer&) = delete;
  AdminServer& operator=(const AdminServer&) = delete;

  [[nodiscard]] std::optional<std::string> start(std::string_view bind_address,
                                                 std::uint16_t port);
  [[nodiscard]] std::uint16_t bound_port() const noexcept;
  void stop();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace forgekv::server
