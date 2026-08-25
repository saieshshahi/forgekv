#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace forgekv::net {

struct ListenerResult {
  int fd{-1};
  std::uint16_t port{0U};
  std::string error;

  [[nodiscard]] bool ok() const noexcept { return fd >= 0; }
};

[[nodiscard]] ListenerResult create_listener(std::string_view address,
                                             std::uint16_t port);
[[nodiscard]] bool set_tcp_nodelay(int fd) noexcept;
[[nodiscard]] bool would_block(int error_number) noexcept;

}  // namespace forgekv::net
