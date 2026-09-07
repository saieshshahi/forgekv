#include "chaos/verifier.h"

#include <arpa/inet.h>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <utility>

namespace forgekv::chaos {
namespace {

constexpr std::size_t kMaximumHttpBytes = 4U * 1024U * 1024U;

bool parse_u64(const std::string_view text, std::uint64_t& value) {
  if (text.empty()) {
    return false;
  }
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), value);
  return error == std::errc{} && end == text.data() + text.size();
}

struct HttpResult final {
  int status{};
  std::string body;
  std::string error;
  [[nodiscard]] bool ok() const noexcept { return error.empty(); }
};

class Socket final {
 public:
  explicit Socket(const int descriptor) : descriptor_(descriptor) {}
  ~Socket() {
    if (descriptor_ >= 0) {
      static_cast<void>(::close(descriptor_));
    }
  }
  Socket(const Socket&) = delete;
  Socket& operator=(const Socket&) = delete;
  [[nodiscard]] int get() const noexcept { return descriptor_; }

 private:
  int descriptor_;
};

HttpResult http_get(const std::uint16_t port, const std::string_view path,
                    const std::chrono::milliseconds timeout) {
  Socket socket(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0));
  if (socket.get() < 0) {
    return {.error = "socket failed"};
  }
  const auto seconds = timeout.count() / 1000;
  const auto micros = (timeout.count() % 1000) * 1000;
  timeval time{.tv_sec = static_cast<time_t>(seconds),
               .tv_usec = static_cast<suseconds_t>(micros)};
  static_cast<void>(::setsockopt(socket.get(), SOL_SOCKET, SO_RCVTIMEO, &time,
                                 sizeof(time)));
  static_cast<void>(::setsockopt(socket.get(), SOL_SOCKET, SO_SNDTIMEO, &time,
                                 sizeof(time)));
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(port);
  if (::connect(socket.get(), reinterpret_cast<sockaddr*>(&address),
                sizeof(address)) != 0) {
    return {.error = "connect failed"};
  }
  const std::string request = "GET " + std::string(path) +
                              " HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: "
                              "close\r\n\r\n";
  std::size_t sent = 0U;
  while (sent < request.size()) {
    const auto count = ::send(socket.get(), request.data() + sent,
                              request.size() - sent, MSG_NOSIGNAL);
    if (count < 0 && errno == EINTR) {
      continue;
    }
    if (count <= 0) {
      return {.error = "send failed"};
    }
    sent += static_cast<std::size_t>(count);
  }
  std::string response;
  std::byte bytes[4096]{};
  while (true) {
    const auto count = ::recv(socket.get(), bytes, sizeof(bytes), 0);
    if (count < 0 && errno == EINTR) {
      continue;
    }
    if (count < 0) {
      return {.error = "receive failed"};
    }
    if (count == 0) {
      break;
    }
    if (static_cast<std::size_t>(count) >
        kMaximumHttpBytes - std::min(response.size(), kMaximumHttpBytes)) {
      return {.error = "HTTP response exceeds limit"};
    }
    response.append(reinterpret_cast<const char*>(bytes),
                    static_cast<std::size_t>(count));
  }
  const auto separator = response.find("\r\n\r\n");
  const auto line_end = response.find("\r\n");
  if (separator == std::string::npos || line_end == std::string::npos ||
      !response.starts_with("HTTP/1.1 ") || line_end < 12U) {
    return {.error = "malformed HTTP response"};
  }
  int status = 0;
  const auto status_text = std::string_view(response).substr(9U, 3U);
  const auto [end, error] = std::from_chars(
      status_text.data(), status_text.data() + status_text.size(), status);
  if (error != std::errc{} || end != status_text.data() + status_text.size()) {
    return {.error = "malformed HTTP status"};
  }
  return {.status = status, .body = response.substr(separator + 4U)};
}

std::optional<std::pair<std::uint64_t, std::uint64_t>> parse_lag(
    const std::string_view line) {
  constexpr std::string_view prefix =
      "forgekv_raft_replication_lag{peer=\"";
  if (!line.starts_with(prefix)) {
    return std::nullopt;
  }
  const auto rest = line.substr(prefix.size());
  const auto delimiter = rest.find("\"} ");
  if (delimiter == std::string_view::npos) {
    return std::nullopt;
  }
  std::uint64_t peer = 0U;
  std::uint64_t lag = 0U;
  if (!parse_u64(rest.substr(0U, delimiter), peer) || peer == 0U ||
      !parse_u64(rest.substr(delimiter + 3U), lag)) {
    return std::nullopt;
  }
  return std::pair{peer, lag};
}

}  // namespace

NodeViewResult parse_node_view(const std::uint64_t node, const bool healthy,
                               const bool ready,
                               const std::string_view metrics) {
  if (node == 0U || metrics.size() > kMaximumHttpBytes) {
    return {.error = "invalid node metrics input"};
  }
  NodeOperationalView view{.node = node,
                           .healthy = healthy,
                           .ready = ready};
  bool role_seen = false;
  bool term_seen = false;
  bool leader_seen = false;
  bool commit_seen = false;
  bool applied_seen = false;
  std::size_t offset = 0U;
  while (offset < metrics.size()) {
    const auto end = metrics.find('\n', offset);
    const auto line = metrics.substr(
        offset, end == std::string_view::npos ? metrics.size() - offset
                                              : end - offset);
    offset = end == std::string_view::npos ? metrics.size() : end + 1U;
    if (line.starts_with("forgekv_raft_role")) {
      if (role_seen || !line.ends_with("} 1")) {
        return {.error = "node " + std::to_string(node) +
                         " has malformed or duplicate role"};
      }
      constexpr std::string_view prefix = "forgekv_raft_role{role=\"";
      if (!line.starts_with(prefix)) {
        return {.error = "node " + std::to_string(node) + " has invalid role"};
      }
      const auto role = line.substr(prefix.size(),
                                    line.size() - prefix.size() - 4U);
      if (role == "leader") {
        view.role = ObservedRole::leader;
      } else if (role == "candidate") {
        view.role = ObservedRole::candidate;
      } else if (role == "follower") {
        view.role = ObservedRole::follower;
      } else {
        return {.error = "node " + std::to_string(node) + " has unknown role"};
      }
      role_seen = true;
      continue;
    }
    const auto numeric = [&](const std::string_view name, std::uint64_t& value,
                             bool& seen) -> std::optional<std::string> {
      if (!line.starts_with(name)) {
        return std::nullopt;
      }
      if (seen || line.size() <= name.size() || line[name.size()] != ' ' ||
          !parse_u64(line.substr(name.size() + 1U), value)) {
        return "node " + std::to_string(node) + " has malformed or duplicate " +
               std::string(name);
      }
      seen = true;
      return std::string{};
    };
    if (auto parsed = numeric("forgekv_raft_term", view.term, term_seen);
        parsed.has_value()) {
      if (!parsed->empty()) return {.error = std::move(*parsed)};
      continue;
    }
    if (auto parsed = numeric("forgekv_raft_leader_id", view.leader_id,
                              leader_seen);
        parsed.has_value()) {
      if (!parsed->empty()) return {.error = std::move(*parsed)};
      continue;
    }
    if (auto parsed = numeric("forgekv_raft_commit_index", view.commit_index,
                              commit_seen);
        parsed.has_value()) {
      if (!parsed->empty()) return {.error = std::move(*parsed)};
      continue;
    }
    if (auto parsed = numeric("forgekv_raft_last_applied", view.last_applied,
                              applied_seen);
        parsed.has_value()) {
      if (!parsed->empty()) return {.error = std::move(*parsed)};
      continue;
    }
    if (line.starts_with("forgekv_raft_replication_lag")) {
      const auto lag = parse_lag(line);
      if (!lag.has_value() || !view.peer_lag.emplace(*lag).second) {
        return {.error = "node " + std::to_string(node) +
                         " has malformed or duplicate peer lag"};
      }
    }
  }
  if (!role_seen || !term_seen || !leader_seen || !commit_seen ||
      !applied_seen) {
    return {.error = "node " + std::to_string(node) +
                     " is missing required Raft metrics"};
  }
  return {.view = std::move(view)};
}

ConvergenceStatus evaluate_convergence(
    const std::vector<NodeOperationalView>& views) {
  if (views.empty()) {
    return {.error = "cluster has no node views"};
  }
  const NodeOperationalView* leader = nullptr;
  std::set<std::uint64_t> node_ids;
  for (const auto& view : views) {
    if (view.node == 0U || !node_ids.insert(view.node).second) {
      return {.error = "cluster has invalid or duplicate node identity"};
    }
    if (!view.healthy) {
      return {.error = "node " + std::to_string(view.node) + " is unhealthy"};
    }
    if (view.commit_index != view.last_applied) {
      return {.error = "node " + std::to_string(view.node) +
                       " has unapplied commits"};
    }
    if (view.role == ObservedRole::leader) {
      if (leader != nullptr || !view.ready) {
        return {.error = "cluster does not have exactly one ready leader"};
      }
      leader = &view;
    } else if (view.ready) {
      return {.error = "non-leader node reports ready"};
    }
  }
  if (leader == nullptr) {
    return {.error = "cluster has no ready leader"};
  }
  for (const auto& view : views) {
    if (view.term != leader->term || view.leader_id != leader->node ||
        view.commit_index != leader->commit_index) {
      return {.error = "cluster Raft views have not converged"};
    }
  }
  for (const auto& [peer, lag] : leader->peer_lag) {
    static_cast<void>(peer);
    if (lag != 0U) {
      return {.error = "leader reports nonzero replication lag"};
    }
  }
  if (leader->peer_lag.size() != views.size()) {
    return {.error = "leader has incomplete replication lag labels"};
  }
  for (const auto node : node_ids) {
    if (!leader->peer_lag.contains(node)) {
      return {.error = "leader has incorrect replication lag labels"};
    }
  }
  return {};
}

NodeViewResult fetch_node_view(const NodeAdminEndpoint endpoint,
                               const std::chrono::milliseconds timeout) {
  if (endpoint.node == 0U || endpoint.port == 0U || timeout.count() <= 0) {
    return {.error = "invalid admin endpoint"};
  }
  const auto health = http_get(endpoint.port, "/health", timeout);
  const auto ready = http_get(endpoint.port, "/ready", timeout);
  const auto metrics = http_get(endpoint.port, "/metrics", timeout);
  if (!health.ok() || !ready.ok() || !metrics.ok() || metrics.status != 200) {
    return {.error = "node " + std::to_string(endpoint.node) +
                     " admin endpoint unavailable"};
  }
  return parse_node_view(endpoint.node, health.status == 200,
                         ready.status == 200, metrics.body);
}

AdminTextResult fetch_admin_text(const NodeAdminEndpoint endpoint,
                                 const std::string_view path,
                                 const std::chrono::milliseconds timeout) {
  if (endpoint.node == 0U || endpoint.port == 0U || timeout.count() <= 0 ||
      (path != "/health" && path != "/ready" && path != "/metrics")) {
    return {.error = "invalid admin text request"};
  }
  auto result = http_get(endpoint.port, path, timeout);
  return {.status = result.status,
          .body = std::move(result.body),
          .error = std::move(result.error)};
}

ConvergenceResult wait_for_convergence(
    const std::vector<NodeAdminEndpoint>& endpoints,
    const std::chrono::steady_clock::time_point deadline,
    const std::function<bool()> interrupted) {
  std::string last_error = "convergence deadline expired";
  while (std::chrono::steady_clock::now() < deadline) {
    if (interrupted && interrupted()) {
      return {.error = "convergence interrupted"};
    }
    std::vector<NodeOperationalView> views;
    views.reserve(endpoints.size());
    bool fetched = true;
    for (const auto endpoint : endpoints) {
      if (interrupted && interrupted()) {
        return {.error = "convergence interrupted"};
      }
      const auto now = std::chrono::steady_clock::now();
      if (now >= deadline) {
        break;
      }
      const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
          deadline - now);
      const auto request_timeout =
          std::max(std::chrono::milliseconds(1),
                   std::min(std::chrono::milliseconds(250),
                            remaining / static_cast<std::int64_t>(
                                            endpoints.size() * 3U)));
      auto result = fetch_node_view(endpoint, request_timeout);
      if (!result.ok()) {
        last_error = std::move(result.error);
        fetched = false;
        break;
      }
      views.push_back(std::move(*result.view));
    }
    if (fetched) {
      const auto convergence = evaluate_convergence(views);
      if (convergence.ok()) {
        return {.views = std::move(views)};
      }
      last_error = convergence.error;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return {.error = std::move(last_error)};
}

}  // namespace forgekv::chaos
