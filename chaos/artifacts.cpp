#include "chaos/artifacts.h"

#include <fcntl.h>
#include <unistd.h>

#include <array>
#include <charconv>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <iterator>
#include <limits>
#include <sys/stat.h>
#include <system_error>

namespace forgekv::chaos {
namespace {

ArtifactStatus write_all(const int descriptor, const std::string_view bytes) {
  std::size_t offset = 0U;
  while (offset < bytes.size()) {
    const auto count = ::write(descriptor, bytes.data() + offset,
                               bytes.size() - offset);
    if (count > 0) {
      offset += static_cast<std::size_t>(count);
    } else if (count < 0 && errno == EINTR) {
      continue;
    } else {
      return {.error = std::string("write artifact: ") + std::strerror(errno)};
    }
  }
  return {};
}

std::optional<std::string> read_bounded_file(
    const std::filesystem::path& path, const std::size_t maximum,
    std::string& error) {
  const int descriptor = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (descriptor < 0) {
    error = "open bounded artifact: " + std::string(std::strerror(errno));
    return std::nullopt;
  }
  struct stat metadata {};
  if (::fstat(descriptor, &metadata) != 0 || !S_ISREG(metadata.st_mode) ||
      metadata.st_size < 0 ||
      static_cast<std::uintmax_t>(metadata.st_size) > maximum) {
    static_cast<void>(::close(descriptor));
    error = "artifact exceeds limit or is not a regular file";
    return std::nullopt;
  }
  std::string result(static_cast<std::size_t>(metadata.st_size), '\0');
  std::size_t offset = 0U;
  while (offset < result.size()) {
    const auto count =
        ::read(descriptor, result.data() + offset, result.size() - offset);
    if (count < 0 && errno == EINTR) {
      continue;
    }
    if (count <= 0) {
      static_cast<void>(::close(descriptor));
      error = "truncated artifact read";
      return std::nullopt;
    }
    offset += static_cast<std::size_t>(count);
  }
  char extra = '\0';
  ssize_t extra_count = 0;
  do {
    extra_count = ::read(descriptor, &extra, 1U);
  } while (extra_count < 0 && errno == EINTR);
  static_cast<void>(::close(descriptor));
  if (extra_count != 0) {
    error = "artifact changed or exceeds limit";
    return std::nullopt;
  }
  return result;
}

bool consume(std::string_view& input, const std::string_view expected) {
  if (!input.starts_with(expected)) {
    return false;
  }
  input.remove_prefix(expected.size());
  return true;
}

bool consume_u64(std::string_view& input, std::uint64_t& value) {
  const auto separator = input.find_first_not_of("0123456789");
  const auto digits = input.substr(0U, separator);
  if (digits.empty()) {
    return false;
  }
  const auto parsed = std::from_chars(digits.data(), digits.data() + digits.size(),
                                      value);
  if (parsed.ec != std::errc{} || parsed.ptr != digits.data() + digits.size()) {
    return false;
  }
  input.remove_prefix(digits.size());
  return true;
}

std::optional<ActionKind> parse_kind(const std::string_view name) {
  constexpr std::array kinds{
      ActionKind::no_op,
      ActionKind::kill_leader,
      ActionKind::kill_follower,
      ActionKind::restart_node,
      ActionKind::partition_node,
      ActionKind::partition_leader_majority,
      ActionKind::set_latency,
      ActionKind::set_jitter,
      ActionKind::set_loss,
      ActionKind::heal_network,
      ActionKind::pause_node,
      ActionKind::resume_node,
      ActionKind::rapid_leader_churn,
  };
  for (const auto kind : kinds) {
    if (action_kind_name(kind) == name) {
      return kind;
    }
  }
  return std::nullopt;
}

std::string_view operation_name(const ClientOperation operation) noexcept {
  switch (operation) {
    case ClientOperation::put: return "put";
    case ClientOperation::get: return "get";
    case ClientOperation::delete_key: return "delete";
  }
  return "unknown";
}

std::string_view attempt_kind_name(const AttemptKind kind) noexcept {
  switch (kind) {
    case AttemptKind::ok: return "ok";
    case AttemptKind::not_found: return "not_found";
    case AttemptKind::redirect: return "redirect";
    case AttemptKind::busy: return "busy";
    case AttemptKind::server_error: return "server_error";
    case AttemptKind::timeout: return "timeout";
    case AttemptKind::transport_error: return "transport_error";
    case AttemptKind::protocol_error: return "protocol_error";
  }
  return "unknown";
}

ArtifactStatus append_line(const std::filesystem::path& path,
                           const std::string_view line,
                           const ArtifactLimits limits,
                           std::size_t& records, std::size_t& bytes) {
  if (line.size() > limits.maximum_line_bytes ||
      records >= limits.maximum_records ||
      line.size() > limits.maximum_bytes -
                        std::min(bytes, limits.maximum_bytes)) {
    return {.error = "artifact limit exceeded"};
  }
  const int descriptor =
      ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
  if (descriptor < 0) {
    return {.error = std::string("open artifact: ") + std::strerror(errno)};
  }
  auto status = write_all(descriptor, line);
  if (status.ok() && ::fdatasync(descriptor) != 0) {
    status.error = std::string("sync artifact: ") + std::strerror(errno);
  }
  if (::close(descriptor) != 0 && status.ok()) {
    status.error = std::string("close artifact: ") + std::strerror(errno);
  }
  if (status.ok()) {
    ++records;
    bytes += line.size();
  }
  return status;
}

std::optional<ChaosAction> parse_action_line(std::string_view line) {
  if (!consume(line, "{\"version\":1,\"kind\":\"")) {
    return std::nullopt;
  }
  const auto quote = line.find('"');
  if (quote == std::string_view::npos) {
    return std::nullopt;
  }
  const auto kind = parse_kind(line.substr(0U, quote));
  line.remove_prefix(quote);
  if (!kind.has_value() ||
      !consume(line, "\",\"planned_offset_us\":")) {
    return std::nullopt;
  }
  ChaosAction action{.kind = *kind};
  std::uint64_t observed_start = 0U;
  std::uint64_t observed_finish = 0U;
  std::uint64_t value = 0U;
  if (!consume_u64(line, action.planned_offset_us) ||
      !consume(line, ",\"observed_start_us\":") ||
      !consume_u64(line, observed_start) ||
      !consume(line, ",\"observed_finish_us\":") ||
      !consume_u64(line, observed_finish) ||
      !consume(line, ",\"node\":") || !consume_u64(line, action.node) ||
      !consume(line, ",\"peer\":") || !consume_u64(line, action.peer) ||
      !consume(line, ",\"value\":") || !consume_u64(line, value) ||
      !consume(line, "}") || !line.empty() ||
      value > std::numeric_limits<std::uint32_t>::max() ||
      observed_finish < observed_start) {
    return std::nullopt;
  }
  action.value = static_cast<std::uint32_t>(value);
  return action;
}

bool safe_filename(const std::string_view filename) {
  return !filename.empty() && filename != "." && filename != ".." &&
         filename.find('/') == std::string_view::npos &&
         filename.find('\\') == std::string_view::npos;
}

std::string client_id_hex(const ClientId& id) {
  constexpr std::array<char, 16> hex{
      '0', '1', '2', '3', '4', '5', '6', '7',
      '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  std::string result;
  result.reserve(id.size() * 2U);
  for (const auto byte : id) {
    const auto value = std::to_integer<unsigned int>(byte);
    result.push_back(hex[value >> 4U]);
    result.push_back(hex[value & 0x0FU]);
  }
  return result;
}

}  // namespace

std::string json_string(const std::string_view value) {
  constexpr std::array<char, 16> hex{
      '0', '1', '2', '3', '4', '5', '6', '7',
      '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  std::string result;
  result.reserve(value.size() + 2U);
  result.push_back('"');
  for (const unsigned char byte : value) {
    switch (byte) {
      case '"': result += "\\\""; break;
      case '\\': result += "\\\\"; break;
      case '\b': result += "\\b"; break;
      case '\f': result += "\\f"; break;
      case '\n': result += "\\n"; break;
      case '\r': result += "\\r"; break;
      case '\t': result += "\\t"; break;
      default:
        if (byte < 0x20U) {
          result += "\\u00";
          result.push_back(hex[byte >> 4U]);
          result.push_back(hex[byte & 0x0FU]);
        } else {
          result.push_back(static_cast<char>(byte));
        }
    }
  }
  result.push_back('"');
  return result;
}

std::string_view action_kind_name(const ActionKind kind) noexcept {
  switch (kind) {
    case ActionKind::no_op: return "no_op";
    case ActionKind::kill_leader: return "kill_leader";
    case ActionKind::kill_follower: return "kill_follower";
    case ActionKind::restart_node: return "restart_node";
    case ActionKind::partition_node: return "partition_node";
    case ActionKind::partition_leader_majority:
      return "partition_leader_majority";
    case ActionKind::set_latency: return "set_latency";
    case ActionKind::set_jitter: return "set_jitter";
    case ActionKind::set_loss: return "set_loss";
    case ActionKind::heal_network: return "heal_network";
    case ActionKind::pause_node: return "pause_node";
    case ActionKind::resume_node: return "resume_node";
    case ActionKind::rapid_leader_churn: return "rapid_leader_churn";
  }
  return "unknown";
}

ArtifactWriter::ArtifactWriter(std::filesystem::path directory,
                               const ArtifactLimits limits)
    : directory_(std::move(directory)), limits_(limits) {
  if (limits_.maximum_records == 0U || limits_.maximum_bytes == 0U ||
      limits_.maximum_line_bytes == 0U) {
    throw std::invalid_argument("artifact limits must be positive");
  }
  std::error_code error;
  if (std::filesystem::exists(directory_, error) && !error &&
      !std::filesystem::is_empty(directory_, error)) {
    throw std::invalid_argument("artifact directory must be empty");
  }
  if (error) {
    throw std::invalid_argument("cannot inspect artifact directory");
  }
  std::filesystem::create_directories(directory_);
}

ArtifactStatus ArtifactWriter::append_action(
    const ChaosAction& action, const std::uint64_t observed_start_us,
    const std::uint64_t observed_finish_us) {
  const std::lock_guard lock(mutex_);
  if (observed_finish_us < observed_start_us) {
    return {.error = "action finish precedes start"};
  }
  const auto line = "{\"version\":1,\"kind\":" +
                    json_string(action_kind_name(action.kind)) +
                    ",\"planned_offset_us\":" +
                    std::to_string(action.planned_offset_us) +
                    ",\"observed_start_us\":" +
                    std::to_string(observed_start_us) +
                    ",\"observed_finish_us\":" +
                    std::to_string(observed_finish_us) + ",\"node\":" +
                    std::to_string(action.node) + ",\"peer\":" +
                    std::to_string(action.peer) + ",\"value\":" +
                    std::to_string(action.value) + "}\n";
  return append_line(directory_ / "timeline.jsonl", line, limits_,
                     timeline_records_, timeline_bytes_);
}

ArtifactStatus ArtifactWriter::append_attempt(const AttemptRecord& attempt) {
  const std::lock_guard lock(mutex_);
  if (attempt.client.empty() || attempt.request.request_id == 0U ||
      attempt.endpoint.port == 0U ||
      attempt.observed_finish_us < attempt.observed_start_us) {
    return {.error = "invalid attempt record"};
  }
  const auto line =
      "{\"version\":1,\"sequence\":" +
      std::to_string(history_records_ + 1U) + ",\"client\":" +
      json_string(attempt.client) +
      ",\"client_id\":" +
      json_string(client_id_hex(attempt.request.client_id)) +
      ",\"request_id\":" + std::to_string(attempt.request.request_id) +
      ",\"op\":" + json_string(operation_name(attempt.request.operation)) +
      ",\"key\":" + json_string(attempt.request.key) +
      ",\"value\":" + json_string(attempt.request.value) +
      ",\"endpoint\":" +
      json_string(attempt.endpoint.host + ":" +
                  std::to_string(attempt.endpoint.port)) +
      ",\"result\":" + json_string(attempt_kind_name(attempt.result)) +
      ",\"diagnostic\":" + json_string(attempt.diagnostic) +
      ",\"observed_start_us\":" +
      std::to_string(attempt.observed_start_us) +
      ",\"observed_finish_us\":" +
      std::to_string(attempt.observed_finish_us) + "}\n";
  return append_line(directory_ / "history.jsonl", line, limits_,
                     history_records_, history_bytes_);
}

ArtifactStatus ArtifactWriter::publish(const std::string_view filename,
                                       const std::string_view contents) const {
  const std::lock_guard lock(mutex_);
  if (!safe_filename(filename) || contents.size() > limits_.maximum_bytes) {
    return {.error = "invalid artifact publication"};
  }
  const auto target = directory_ / filename;
  auto temporary = target;
  temporary += ".tmp";
  const int descriptor = ::open(temporary.c_str(),
                                O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (descriptor < 0) {
    return {.error = std::string("open temporary artifact: ") +
                     std::strerror(errno)};
  }
  auto status = write_all(descriptor, contents);
  if (status.ok() && ::fdatasync(descriptor) != 0) {
    status.error = std::string("sync artifact: ") + std::strerror(errno);
  }
  if (::close(descriptor) != 0 && status.ok()) {
    status.error = std::string("close artifact: ") + std::strerror(errno);
  }
  if (!status.ok()) {
    static_cast<void>(::unlink(temporary.c_str()));
    return status;
  }
  std::error_code error;
  std::filesystem::rename(temporary, target, error);
  if (error) {
    static_cast<void>(::unlink(temporary.c_str()));
    return {.error = "rename artifact: " + error.message()};
  }
  const int directory = ::open(directory_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (directory < 0) {
    return {.error = std::string("open artifact directory: ") +
                     std::strerror(errno)};
  }
  if (::fsync(directory) != 0) {
    status.error = std::string("sync artifact directory: ") +
                   std::strerror(errno);
  }
  static_cast<void>(::close(directory));
  return status;
}

TimelineResult read_timeline(const std::filesystem::path& path,
                             const ArtifactLimits limits) {
  std::string error;
  const auto input = read_bounded_file(path, limits.maximum_bytes, error);
  if (!input.has_value()) {
    return {.error = std::move(error)};
  }
  if (input->empty()) {
    return {.error = "empty timeline"};
  }
  if (input->back() != '\n') {
    return {.error = "truncated timeline"};
  }
  TimelineResult result;
  std::string_view remaining(*input);
  while (!remaining.empty()) {
    const auto newline = remaining.find('\n');
    if (newline == std::string_view::npos ||
        newline > limits.maximum_line_bytes ||
        result.actions.size() >= limits.maximum_records) {
      return {.error = "timeline limit exceeded"};
    }
    const auto action = parse_action_line(remaining.substr(0U, newline));
    if (!action.has_value()) {
      return {.error = "invalid timeline record"};
    }
    result.actions.push_back(*action);
    remaining.remove_prefix(newline + 1U);
  }
  return result;
}

CampaignConfigResult read_campaign_config(const std::filesystem::path& path) {
  std::string error;
  auto contents = read_bounded_file(path, 4096U, error);
  if (!contents.has_value()) {
    return {.error = std::move(error)};
  }
  std::string_view text(*contents);
  if (text.size() > 4096U ||
      !consume(text, "{\"version\":1,\"nodes\":")) {
    return {.error = "invalid campaign config"};
  }
  std::uint64_t nodes = 0U;
  std::uint64_t clients = 0U;
  CampaignConfig config;
  if (!consume_u64(text, nodes) || !consume(text, ",\"clients\":") ||
      !consume_u64(text, clients) ||
      !consume(text, ",\"duration_ms\":") ||
      !consume_u64(text, config.duration_ms) ||
      !consume(text, ",\"action_interval_ms\":") ||
      !consume_u64(text, config.action_interval_ms) ||
      !consume(text, ",\"seed\":") || !consume_u64(text, config.seed) ||
      !consume(text, "}\n") || !text.empty() ||
      (nodes != 3U && nodes != 5U) || clients == 0U || clients > 256U ||
      config.duration_ms < 1000U || config.duration_ms > 3'600'000U ||
      config.action_interval_ms < 50U ||
      config.action_interval_ms > 60'000U) {
    return {.error = "invalid campaign config"};
  }
  config.nodes = static_cast<std::size_t>(nodes);
  config.clients = static_cast<std::size_t>(clients);
  return {.config = config};
}

}  // namespace forgekv::chaos
