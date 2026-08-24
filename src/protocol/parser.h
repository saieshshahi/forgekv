#pragma once

#include "protocol/checksum.h"
#include "protocol/frame.h"

#include <array>
#include <cstddef>
#include <span>
#include <vector>

namespace forgekv::protocol {

struct ParseBatch {
  std::vector<Frame> frames;
  ProtocolError error{ProtocolError::none};

  [[nodiscard]] bool ok() const noexcept { return error == ProtocolError::none; }
};

class Parser final {
 public:
  [[nodiscard]] ParseBatch consume(std::span<const std::byte> input);
  [[nodiscard]] ProtocolError finish() noexcept;
  [[nodiscard]] bool failed() const noexcept { return failed_; }
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;

 private:
  [[nodiscard]] ProtocolError decode_header();
  [[nodiscard]] ProtocolError complete_frame(std::vector<Frame>& frames);
  [[nodiscard]] ParseBatch fail(ProtocolError error, std::vector<Frame> frames);
  void reset_frame_state();

  std::array<std::byte, kHeaderSize> header_{};
  std::size_t header_bytes_{0U};
  FrameHeader current_header_{};
  std::vector<std::byte> payload_;
  std::size_t payload_bytes_{0U};
  Crc32 checksum_{};
  bool awaiting_payload_{false};
  bool failed_{false};
};

}  // namespace forgekv::protocol
