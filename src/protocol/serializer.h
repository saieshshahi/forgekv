#pragma once

#include "protocol/frame.h"

#include <vector>

namespace forgekv::protocol {

struct SerializeResult {
  std::vector<std::byte> bytes;
  ProtocolError error{ProtocolError::none};

  [[nodiscard]] bool ok() const noexcept { return error == ProtocolError::none; }
};

[[nodiscard]] SerializeResult serialize(const Frame& frame);

}  // namespace forgekv::protocol
