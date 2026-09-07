#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace forgekv::chaos {

enum class ObservedRole : std::uint8_t { follower, candidate, leader };

struct NodeOperationalView final {
  std::uint64_t node{};
  bool healthy{};
  bool ready{};
  ObservedRole role{ObservedRole::follower};
  std::uint64_t term{};
  std::uint64_t leader_id{};
  std::uint64_t commit_index{};
  std::uint64_t last_applied{};
  std::map<std::uint64_t, std::uint64_t> peer_lag;
};

struct NodeViewResult final {
  std::optional<NodeOperationalView> view;
  std::string error;
  [[nodiscard]] bool ok() const noexcept { return view.has_value(); }
};

struct ConvergenceStatus final {
  std::string error;
  [[nodiscard]] bool ok() const noexcept { return error.empty(); }
};

struct NodeAdminEndpoint final {
  std::uint64_t node{};
  std::uint16_t port{};
};

struct ConvergenceResult final {
  std::vector<NodeOperationalView> views;
  std::string error;
  [[nodiscard]] bool ok() const noexcept { return error.empty(); }
};

struct AdminTextResult final {
  int status{};
  std::string body;
  std::string error;
  [[nodiscard]] bool ok() const noexcept { return error.empty(); }
};

[[nodiscard]] NodeViewResult parse_node_view(std::uint64_t node, bool healthy,
                                             bool ready,
                                             std::string_view metrics);
[[nodiscard]] ConvergenceStatus evaluate_convergence(
    const std::vector<NodeOperationalView>& views);
[[nodiscard]] NodeViewResult fetch_node_view(
    NodeAdminEndpoint endpoint,
    std::chrono::milliseconds timeout = std::chrono::seconds(1));
[[nodiscard]] AdminTextResult fetch_admin_text(
    NodeAdminEndpoint endpoint, std::string_view path,
    std::chrono::milliseconds timeout = std::chrono::seconds(1));
[[nodiscard]] ConvergenceResult wait_for_convergence(
    const std::vector<NodeAdminEndpoint>& endpoints,
    std::chrono::steady_clock::time_point deadline,
    std::function<bool()> interrupted = {},
    std::chrono::milliseconds maximum_request_timeout =
        std::chrono::milliseconds(250));

}  // namespace forgekv::chaos
