#include "storage/wal_record.h"

#include <algorithm>
#include <limits>
#include <stdexcept>

#include "protocol/checksum.h"
#include "protocol/wire.h"

namespace forgekv::storage {
namespace {

namespace wire = forgekv::protocol::wire;

void write_u16(const std::span<std::byte> destination, const std::uint16_t value) {
  destination[0] = static_cast<std::byte>((value >> 8U) & 0xFFU);
  destination[1] = static_cast<std::byte>(value & 0xFFU);
}

std::uint16_t read_u16(const std::span<const std::byte> source) {
  return static_cast<std::uint16_t>(
      (std::to_integer<std::uint16_t>(source[0]) << 8U) |
      std::to_integer<std::uint16_t>(source[1]));
}

std::uint32_t record_checksum(const std::span<const std::byte> bytes) {
  protocol::Crc32 checksum;
  checksum.update(bytes.first(32U));
  checksum.update(bytes.subspan(kWalHeaderSize));
  return checksum.value();
}

std::string validation_error(const WalRecord& record) {
  if (record.lsn == 0U) {
    return "LSN must be positive";
  }
  if (record.key.empty() || record.key.size() > kMaxKeySize) {
    return "key length is outside the supported range";
  }
  if (record.value.size() > kMaxValueSize) {
    return "value exceeds the maximum size";
  }
  if (record.operation != WalOperation::put &&
      record.operation != WalOperation::delete_key) {
    return "unknown WAL operation";
  }
  if (record.operation == WalOperation::delete_key && !record.value.empty()) {
    return "DELETE records cannot contain values";
  }
  return {};
}

DecodeResult corrupt(std::string error) {
  return {DecodeStatus::corrupt, std::nullopt, 0U, std::move(error)};
}

}  // namespace

std::vector<std::byte> encode_record(const WalRecord& record) {
  if (const auto error = validation_error(record); !error.empty()) {
    throw std::invalid_argument(error);
  }

  const auto record_size = kWalHeaderSize + record.key.size() + record.value.size();
  if (record_size > std::numeric_limits<std::uint32_t>::max()) {
    throw std::length_error("WAL record does not fit the wire length field");
  }

  std::vector<std::byte> bytes(record_size);
  wire::write_u32(std::span{bytes}.subspan(0U, 4U), kWalMagic);
  write_u16(std::span{bytes}.subspan(4U, 2U), kWalVersion);
  write_u16(std::span{bytes}.subspan(6U, 2U),
            static_cast<std::uint16_t>(kWalHeaderSize));
  wire::write_u32(std::span{bytes}.subspan(8U, 4U),
                  static_cast<std::uint32_t>(record_size));
  wire::write_u64(std::span{bytes}.subspan(12U, 8U), record.lsn);
  bytes[20U] = static_cast<std::byte>(record.operation);
  wire::write_u32(std::span{bytes}.subspan(24U, 4U),
                  static_cast<std::uint32_t>(record.key.size()));
  wire::write_u32(std::span{bytes}.subspan(28U, 4U),
                  static_cast<std::uint32_t>(record.value.size()));
  std::ranges::copy(std::as_bytes(std::span{record.key}),
                    bytes.begin() + static_cast<std::ptrdiff_t>(kWalHeaderSize));
  std::ranges::copy(
      std::as_bytes(std::span{record.value}),
      bytes.begin() + static_cast<std::ptrdiff_t>(kWalHeaderSize + record.key.size()));
  wire::write_u32(std::span{bytes}.subspan(32U, 4U), record_checksum(bytes));
  return bytes;
}

DecodeResult decode_record(const std::span<const std::byte> bytes) {
  if (bytes.size() < kWalHeaderSize) {
    return {DecodeStatus::incomplete, std::nullopt, 0U, {}};
  }
  if (wire::read_u32(bytes.subspan(0U, 4U)) != kWalMagic) {
    return corrupt("invalid WAL magic");
  }
  if (read_u16(bytes.subspan(4U, 2U)) != kWalVersion) {
    return corrupt("unsupported WAL version");
  }
  if (read_u16(bytes.subspan(6U, 2U)) != kWalHeaderSize) {
    return corrupt("invalid WAL header length");
  }
  if (bytes[21U] != std::byte{0U} || bytes[22U] != std::byte{0U} ||
      bytes[23U] != std::byte{0U}) {
    return corrupt("reserved WAL fields are nonzero");
  }

  const auto record_size = wire::read_u32(bytes.subspan(8U, 4U));
  const auto key_size = wire::read_u32(bytes.subspan(24U, 4U));
  const auto value_size = wire::read_u32(bytes.subspan(28U, 4U));
  if (record_size < kWalHeaderSize || record_size > kMaxWalRecordSize) {
    return corrupt("invalid WAL record length");
  }
  if (key_size == 0U || key_size > kMaxKeySize || value_size > kMaxValueSize) {
    return corrupt("invalid WAL payload lengths");
  }
  const auto expected_size = kWalHeaderSize + static_cast<std::size_t>(key_size) +
                             static_cast<std::size_t>(value_size);
  if (record_size != expected_size) {
    return corrupt("WAL record and payload lengths disagree");
  }
  const auto lsn = wire::read_u64(bytes.subspan(12U, 8U));
  if (lsn == 0U) {
    return corrupt("invalid zero LSN");
  }
  const auto operation = static_cast<WalOperation>(std::to_integer<std::uint8_t>(bytes[20U]));
  if (operation != WalOperation::put && operation != WalOperation::delete_key) {
    return corrupt("unknown WAL operation");
  }
  if (operation == WalOperation::delete_key && value_size != 0U) {
    return corrupt("DELETE record contains a value");
  }
  if (bytes.size() < record_size) {
    return {DecodeStatus::incomplete, std::nullopt, 0U, {}};
  }
  const auto complete_bytes = bytes.first(record_size);
  if (wire::read_u32(complete_bytes.subspan(32U, 4U)) !=
      record_checksum(complete_bytes)) {
    return corrupt("WAL checksum mismatch");
  }

  const auto payload = complete_bytes.subspan(kWalHeaderSize);
  WalRecord record{lsn, operation,
                   std::string(reinterpret_cast<const char*>(payload.data()), key_size),
                   std::string(reinterpret_cast<const char*>(payload.data() + key_size),
                               value_size)};
  return {DecodeStatus::complete, std::move(record), record_size, {}};
}

}  // namespace forgekv::storage
