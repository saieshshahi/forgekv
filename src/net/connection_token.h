#pragma once

#include <cstdint>

namespace forgekv::net {

struct ConnectionToken {
  std::uint64_t id{0U};
  std::uint64_t generation{0U};

  bool operator==(const ConnectionToken&) const = default;
};

}  // namespace forgekv::net
