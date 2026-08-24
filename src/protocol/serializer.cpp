#include "protocol/serializer.h"

#include "protocol/checksum.h"
#include "protocol/wire.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <span>

namespace forgekv::protocol {
namespace {

ProtocolError validate(const Frame& frame) {
  if (!is_valid_namespace(frame.message_namespace)) {
    return ProtocolError::invalid_namespace;
  }
  if (!is_valid_message_type(frame.message_type)) {
    return ProtocolError::invalid_message_type;
  }
  if (!message_type_matches_namespace(frame.message_namespace, frame.message_type)) {
    return ProtocolError::message_type_namespace_mismatch;
  }
  if (frame.flags != 0U) {
    return ProtocolError::unsupported_flags;
  }
  if (frame.payload.size() > kMaxPayloadSize) {
    return ProtocolError::payload_too_large;
  }
  return ProtocolError::none;
}

}  // namespace

SerializeResult serialize(const Frame& frame) {
  const auto validation_error = validate(frame);
  if (validation_error != ProtocolError::none) {
    return {.bytes = {}, .error = validation_error};
  }

  std::vector<std::byte> bytes(kHeaderSize + frame.payload.size());
  auto output = std::span<std::byte>(bytes);
  wire::write_u32(output.subspan<0U, 4U>(), kMagic);
  output[4] = static_cast<std::byte>(kVersion);
  output[5] = static_cast<std::byte>(frame.message_namespace);
  output[6] = static_cast<std::byte>(frame.message_type);
  output[7] = static_cast<std::byte>(frame.flags);
  wire::write_u64(output.subspan<8U, 8U>(), frame.request_id);
  wire::write_u32(output.subspan<16U, 4U>(),
                  static_cast<std::uint32_t>(frame.payload.size()));
  std::ranges::copy(frame.payload, output.begin() + static_cast<std::ptrdiff_t>(kHeaderSize));

  Crc32 checksum;
  checksum.update(std::span<const std::byte>(bytes).first(20U));
  checksum.update(std::span<const std::byte>(bytes).subspan(kHeaderSize));
  wire::write_u32(output.subspan<20U, 4U>(), checksum.value());

  return {.bytes = std::move(bytes), .error = ProtocolError::none};
}

}  // namespace forgekv::protocol
