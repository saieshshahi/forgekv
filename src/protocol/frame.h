#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

namespace forgekv::protocol {

inline constexpr std::uint32_t kMagic = 0x464B5631U;
inline constexpr std::uint8_t kVersion = 1U;
inline constexpr std::size_t kHeaderSize = 24U;
inline constexpr std::size_t kMaxKeySize = 1U << 10U;
inline constexpr std::size_t kMaxValueSize = 1U << 20U;
inline constexpr std::size_t kMaxPayloadSize = kMaxKeySize + kMaxValueSize + 64U;
inline constexpr std::size_t kMaxFrameSize = kHeaderSize + kMaxPayloadSize;

static_assert(kHeaderSize == 24U);
static_assert(kMaxPayloadSize <= std::numeric_limits<std::uint32_t>::max());
static_assert(kMaxFrameSize > kMaxPayloadSize);

enum class Namespace : std::uint8_t {
  client = 1,
  raft = 2,
};

enum class MessageType : std::uint8_t {
  put = 0x01,
  get = 0x02,
  delete_key = 0x03,
  ping = 0x04,

  raft_append_entries = 0x40,
  raft_request_vote = 0x41,
  raft_install_snapshot = 0x42,

  ok = 0x80,
  not_found = 0x81,
  error = 0x82,
  redirect = 0x83,
  busy = 0x84,
};

enum class ProtocolError : std::uint8_t {
  none = 0,
  invalid_magic,
  unsupported_version,
  invalid_namespace,
  invalid_message_type,
  message_type_namespace_mismatch,
  unsupported_flags,
  payload_too_large,
  checksum_mismatch,
  truncated_frame,
  parser_failed,
};

[[nodiscard]] constexpr bool is_valid_namespace(const Namespace message_namespace) noexcept {
  return message_namespace == Namespace::client || message_namespace == Namespace::raft;
}

[[nodiscard]] constexpr bool is_client_message_type(const MessageType message_type) noexcept {
  switch (message_type) {
    case MessageType::put:
    case MessageType::get:
    case MessageType::delete_key:
    case MessageType::ping:
    case MessageType::ok:
    case MessageType::not_found:
    case MessageType::error:
    case MessageType::redirect:
    case MessageType::busy:
      return true;
    case MessageType::raft_append_entries:
    case MessageType::raft_request_vote:
    case MessageType::raft_install_snapshot:
      return false;
  }
  return false;
}

[[nodiscard]] constexpr bool is_raft_message_type(const MessageType message_type) noexcept {
  return message_type == MessageType::raft_append_entries ||
         message_type == MessageType::raft_request_vote ||
         message_type == MessageType::raft_install_snapshot;
}

[[nodiscard]] constexpr bool is_valid_message_type(const MessageType message_type) noexcept {
  return is_client_message_type(message_type) || is_raft_message_type(message_type);
}

[[nodiscard]] constexpr bool message_type_matches_namespace(
    const Namespace message_namespace, const MessageType message_type) noexcept {
  return (message_namespace == Namespace::client && is_client_message_type(message_type)) ||
         (message_namespace == Namespace::raft && is_raft_message_type(message_type));
}

struct FrameHeader {
  Namespace message_namespace{Namespace::client};
  MessageType message_type{MessageType::ping};
  std::uint8_t flags{0U};
  std::uint64_t request_id{0U};
  std::uint32_t payload_length{0U};
  std::uint32_t checksum{0U};

  bool operator==(const FrameHeader&) const = default;
};

struct Frame {
  Namespace message_namespace{Namespace::client};
  MessageType message_type{MessageType::ping};
  std::uint8_t flags{0U};
  std::uint64_t request_id{0U};
  std::vector<std::byte> payload;

  bool operator==(const Frame&) const = default;
};

}  // namespace forgekv::protocol
