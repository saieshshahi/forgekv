#include "protocol/parser.h"

#include "protocol/wire.h"

#include <algorithm>
#include <cstddef>
#include <span>
#include <utility>

namespace forgekv::protocol {

ParseBatch Parser::consume(const std::span<const std::byte> input) {
  if (failed_) {
    return {.frames = {}, .error = ProtocolError::parser_failed};
  }

  std::vector<Frame> frames;
  std::size_t offset = 0U;
  while (offset < input.size()) {
    if (!awaiting_payload_) {
      const auto count = std::min(kHeaderSize - header_bytes_, input.size() - offset);
      std::ranges::copy(input.subspan(offset, count), header_.begin() +
                                                       static_cast<std::ptrdiff_t>(header_bytes_));
      header_bytes_ += count;
      offset += count;
      if (header_bytes_ != kHeaderSize) {
        break;
      }

      const auto error = decode_header();
      if (error != ProtocolError::none) {
        return fail(error, std::move(frames));
      }

      payload_.resize(current_header_.payload_length);
      payload_bytes_ = 0U;
      checksum_ = Crc32{};
      checksum_.update(std::span<const std::byte>(header_).first(20U));
      awaiting_payload_ = true;

      if (payload_.empty()) {
        const auto completion_error = complete_frame(frames);
        if (completion_error != ProtocolError::none) {
          return fail(completion_error, std::move(frames));
        }
      }
    }

    if (awaiting_payload_ && offset < input.size()) {
      const auto count = std::min(payload_.size() - payload_bytes_, input.size() - offset);
      const auto chunk = input.subspan(offset, count);
      std::ranges::copy(chunk,
                        payload_.begin() + static_cast<std::ptrdiff_t>(payload_bytes_));
      checksum_.update(chunk);
      payload_bytes_ += count;
      offset += count;

      if (payload_bytes_ == payload_.size()) {
        const auto completion_error = complete_frame(frames);
        if (completion_error != ProtocolError::none) {
          return fail(completion_error, std::move(frames));
        }
      }
    }
  }

  return {.frames = std::move(frames), .error = ProtocolError::none};
}

ProtocolError Parser::finish() noexcept {
  if (failed_) {
    return ProtocolError::parser_failed;
  }
  if (header_bytes_ != 0U || awaiting_payload_) {
    failed_ = true;
    reset_frame_state();
    return ProtocolError::truncated_frame;
  }
  return ProtocolError::none;
}

std::size_t Parser::buffered_bytes() const noexcept {
  return header_bytes_ + payload_bytes_;
}

ProtocolError Parser::decode_header() {
  const auto bytes = std::span<const std::byte>(header_);
  if (wire::read_u32(bytes.subspan<0U, 4U>()) != kMagic) {
    return ProtocolError::invalid_magic;
  }
  if (std::to_integer<unsigned int>(bytes[4]) != kVersion) {
    return ProtocolError::unsupported_version;
  }

  const auto message_namespace =
      static_cast<Namespace>(std::to_integer<std::uint8_t>(bytes[5]));
  if (!is_valid_namespace(message_namespace)) {
    return ProtocolError::invalid_namespace;
  }

  const auto message_type =
      static_cast<MessageType>(std::to_integer<std::uint8_t>(bytes[6]));
  if (!is_valid_message_type(message_type)) {
    return ProtocolError::invalid_message_type;
  }
  if (!message_type_matches_namespace(message_namespace, message_type)) {
    return ProtocolError::message_type_namespace_mismatch;
  }

  const auto flags = std::to_integer<std::uint8_t>(bytes[7]);
  if (flags != 0U) {
    return ProtocolError::unsupported_flags;
  }

  const auto payload_length = wire::read_u32(bytes.subspan<16U, 4U>());
  if (payload_length > kMaxPayloadSize) {
    return ProtocolError::payload_too_large;
  }

  current_header_ = FrameHeader{
      .message_namespace = message_namespace,
      .message_type = message_type,
      .flags = flags,
      .request_id = wire::read_u64(bytes.subspan<8U, 8U>()),
      .payload_length = payload_length,
      .checksum = wire::read_u32(bytes.subspan<20U, 4U>()),
  };
  return ProtocolError::none;
}

ProtocolError Parser::complete_frame(std::vector<Frame>& frames) {
  if (checksum_.value() != current_header_.checksum) {
    return ProtocolError::checksum_mismatch;
  }

  frames.push_back(Frame{
      .message_namespace = current_header_.message_namespace,
      .message_type = current_header_.message_type,
      .flags = current_header_.flags,
      .request_id = current_header_.request_id,
      .payload = std::move(payload_),
  });
  reset_frame_state();
  return ProtocolError::none;
}

ParseBatch Parser::fail(const ProtocolError error, std::vector<Frame> frames) {
  failed_ = true;
  reset_frame_state();
  return {.frames = std::move(frames), .error = error};
}

void Parser::reset_frame_state() {
  header_bytes_ = 0U;
  current_header_ = FrameHeader{};
  payload_.clear();
  payload_bytes_ = 0U;
  checksum_ = Crc32{};
  awaiting_payload_ = false;
}

}  // namespace forgekv::protocol
