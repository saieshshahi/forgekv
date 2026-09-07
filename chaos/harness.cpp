#include "chaos/harness.h"

#include "chaos/artifacts.h"
#include "chaos/client_worker.h"
#include "chaos/process_cluster.h"
#include "chaos/scheduler.h"
#include "chaos/verifier.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace forgekv::chaos {
namespace {

using Clock = std::chrono::steady_clock;

constexpr std::uintmax_t kMaximumArtifactBytes = 2ULL * 1024ULL * 1024ULL * 1024ULL;
constexpr std::uintmax_t kMaximumServerLogBytes = 64ULL * 1024ULL * 1024ULL;

std::uint64_t elapsed_us(const Clock::time_point start) {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - start)
          .count());
}

ClientId make_client_id(const std::uint64_t seed, const std::size_t index) {
  ClientId id{};
  std::uint64_t left = seed;
  std::uint64_t right = static_cast<std::uint64_t>(index + 1U);
  for (std::size_t byte = 0U; byte < 8U; ++byte) {
    id[byte] = static_cast<std::byte>(left & 0xFFU);
    id[byte + 8U] = static_cast<std::byte>(right & 0xFFU);
    left >>= 8U;
    right >>= 8U;
  }
  return id;
}

std::vector<NodeAdminEndpoint> admin_endpoints(ProcessCluster& cluster,
                                                const std::size_t count) {
  std::vector<NodeAdminEndpoint> endpoints;
  endpoints.reserve(count);
  for (std::size_t index = 0U; index < count; ++index) {
    const auto node = static_cast<std::uint64_t>(index + 1U);
    endpoints.push_back(
        NodeAdminEndpoint{.node = node, .port = cluster.admin_port(node)});
  }
  return endpoints;
}

std::optional<std::uint64_t> observed_leader(ProcessCluster& cluster,
                                             const std::size_t count,
                                             const std::chrono::milliseconds timeout =
                                                 std::chrono::milliseconds(100)) {
  for (std::size_t index = 0U; index < count; ++index) {
    const auto node = static_cast<std::uint64_t>(index + 1U);
    if (cluster.state(node) == NodeState::dead) {
      continue;
    }
    const auto view = fetch_node_view(
        NodeAdminEndpoint{.node = node, .port = cluster.admin_port(node)},
        timeout);
    if (view.ok() && view.view->role == ObservedRole::leader) {
      return node;
    }
  }
  return std::nullopt;
}

ClusterView current_cluster_view(ProcessCluster& cluster,
                                 const std::size_t count,
                                 const std::chrono::milliseconds timeout) {
  auto view = cluster.refresh();
  view.leader = observed_leader(cluster, count, timeout);
  return view;
}

Endpoint endpoint_for(ProcessCluster& cluster, const std::uint64_t node) {
  return Endpoint{.host = "127.0.0.1", .port = cluster.client_port(node)};
}

std::uint64_t first_node_with_state(const ClusterView& view,
                                    const NodeState state) {
  const auto found = std::ranges::find(view.nodes, state, &NodeView::state);
  return found == view.nodes.end() ? 0U : found->id;
}

std::string shell_quote(const std::string& value) {
  std::string result{"'"};
  for (const char character : value) {
    if (character == '\'') {
      result += "'\\''";
    } else {
      result.push_back(character);
    }
  }
  result.push_back('\'');
  return result;
}

AdminTextResult fetch_admin_text_until(
    const NodeAdminEndpoint endpoint, const std::string_view path,
    const Clock::time_point deadline,
    const std::chrono::milliseconds request_timeout,
    const std::function<bool()>& interrupted) {
  AdminTextResult last{.status = 0,
                       .body = {},
                       .error = "admin collection deadline expired"};
  while (Clock::now() < deadline && !(interrupted && interrupted())) {
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
        deadline - Clock::now());
    last = fetch_admin_text(
        endpoint, path,
        std::max(std::chrono::milliseconds(1),
                 std::min(request_timeout, remaining)));
    if (last.ok()) return last;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return last;
}

ClusterStatus validate_artifact_bounds(const std::filesystem::path& root) {
  std::error_code error;
  std::uintmax_t total = 0U;
  for (std::filesystem::recursive_directory_iterator iterator(root, error), end;
       iterator != end && !error; iterator.increment(error)) {
    const auto status = iterator->symlink_status(error);
    if (error) {
      break;
    }
    if (std::filesystem::is_symlink(status)) {
      return {.error = "artifact tree contains a symbolic link"};
    }
    if (!std::filesystem::is_regular_file(status)) {
      continue;
    }
    const auto size = iterator->file_size(error);
    if (error || size > kMaximumArtifactBytes -
                            std::min(total, kMaximumArtifactBytes)) {
      return {.error = "artifact byte limit exceeded"};
    }
    total += size;
    if (iterator->path().parent_path() == root / "logs" &&
        size > kMaximumServerLogBytes) {
      return {.error = "server log byte limit exceeded: " +
                       iterator->path().filename().string()};
    }
  }
  if (error) {
    return {.error = "inspect artifact tree: " + error.message()};
  }
  return {};
}

ClusterStatus scan_critical_logs(const std::filesystem::path& root) {
  std::error_code error;
  const auto log_directory = root / "logs";
  if (!std::filesystem::exists(log_directory, error)) {
    return error ? ClusterStatus{.error = "inspect logs: " + error.message()}
                 : ClusterStatus{};
  }
  for (std::filesystem::directory_iterator iterator(log_directory, error), end;
       iterator != end && !error; iterator.increment(error)) {
    if (!iterator->is_regular_file(error)) {
      if (error) break;
      continue;
    }
    const auto size = iterator->file_size(error);
    if (error || size > kMaximumServerLogBytes) {
      return {.error = "server log is unreadable or exceeds limit: " +
                       iterator->path().filename().string()};
    }
    std::ifstream input(iterator->path());
    std::string line;
    while (std::getline(input, line)) {
      if (line.find("\"severity\":\"CRITICAL\"") != std::string::npos ||
          line.find("server_fatal") != std::string::npos ||
          line.find("server_startup_failed") != std::string::npos) {
        if (line.size() > 512U) line.resize(512U);
        return {.error = "critical server log in " +
                         iterator->path().filename().string() + ": " + line};
      }
    }
    if (!input.eof()) {
      return {.error = "read server log: " +
                       iterator->path().filename().string()};
    }
  }
  return error ? ClusterStatus{.error = "scan logs: " + error.message()}
               : ClusterStatus{};
}

std::string failure_category(const std::string_view diagnostic) {
  if (diagnostic.find("interrupted") != std::string_view::npos) return "signal";
  if (diagnostic.find("invalid chaos harness options") !=
      std::string_view::npos) return "configuration";
  if (diagnostic.find("artifact") != std::string_view::npos ||
      diagnostic.find("log byte") != std::string_view::npos ||
      diagnostic.find("metrics snapshot") != std::string_view::npos ||
      diagnostic.find("metrics artifact") != std::string_view::npos) {
    return "artifact I/O";
  }
  if (diagnostic.find("unexpected child exit") != std::string_view::npos ||
      diagnostic.find("critical server log") != std::string_view::npos) {
    return "child crash";
  }
  if (diagnostic.find("protocol") != std::string_view::npos) {
    return "client protocol";
  }
  if (diagnostic.find("client invariant") != std::string_view::npos ||
      diagnostic.find("verification") != std::string_view::npos ||
      diagnostic.find("ambiguous") != std::string_view::npos) return "invariant";
  if (diagnostic.find("convergence") != std::string_view::npos ||
      diagnostic.find("leader") != std::string_view::npos) {
    return "convergence timeout";
  }
  if (diagnostic.find("fork") != std::string_view::npos ||
      diagnostic.find("start") != std::string_view::npos ||
      diagnostic.find("launch") != std::string_view::npos) return "launch";
  return "internal";
}

ClusterStatus apply_action(ProcessCluster& cluster, const ChaosAction& action,
                           const std::size_t node_count) {
  switch (action.kind) {
    case ActionKind::no_op: return {};
    case ActionKind::kill_leader:
    case ActionKind::kill_follower:
      return cluster.kill_node(action.node);
    case ActionKind::rapid_leader_churn: {
      cluster.heal_network();
      if (auto status = cluster.kill_node(action.node); !status.ok()) {
        return status;
      }
      std::optional<std::uint64_t> replacement;
      const auto deadline = Clock::now() + std::chrono::seconds(3);
      while (Clock::now() < deadline) {
        replacement = observed_leader(cluster, node_count);
        if (replacement.has_value() && *replacement != action.node) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
      }
      if (auto status = cluster.restart_node(action.node); !status.ok()) {
        return status;
      }
      if (!replacement.has_value() || *replacement == action.node) {
        return {.error = "rapid leader churn did not elect a replacement"};
      }
      if (auto status = cluster.kill_node(*replacement); !status.ok()) {
        return status;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      return cluster.restart_node(*replacement);
    }
    case ActionKind::restart_node: return cluster.restart_node(action.node);
    case ActionKind::pause_node: return cluster.pause_node(action.node);
    case ActionKind::resume_node: return cluster.resume_node(action.node);
    case ActionKind::heal_network:
      cluster.heal_network();
      return {};
    case ActionKind::set_latency:
      return cluster.set_link_policy(
          action.node, action.peer, LinkPolicy{.latency_ms = action.value});
    case ActionKind::set_jitter:
      return cluster.set_link_policy(
          action.node, action.peer, LinkPolicy{.jitter_ms = action.value});
    case ActionKind::set_loss:
      return cluster.set_link_policy(
          action.node, action.peer,
          LinkPolicy{.loss_percent = action.value});
    case ActionKind::partition_node:
    case ActionKind::partition_leader_majority:
      for (std::size_t other = 1U; other <= node_count; ++other) {
        const auto peer = static_cast<std::uint64_t>(other);
        if (peer == action.node) {
          continue;
        }
        if (auto status = cluster.set_link_policy(
                action.node, peer, LinkPolicy{.partitioned = true});
            !status.ok()) {
          return status;
        }
        if (auto status = cluster.set_link_policy(
                peer, action.node, LinkPolicy{.partitioned = true});
            !status.ok()) {
          return status;
        }
      }
      return {};
  }
  return {.error = "unknown chaos action"};
}

std::optional<std::uint64_t> leader_from(
    const std::vector<NodeOperationalView>& views) {
  const auto found = std::ranges::find(views, ObservedRole::leader,
                                       &NodeOperationalView::role);
  return found == views.end() ? std::nullopt
                              : std::optional<std::uint64_t>(found->node);
}

}  // namespace

ChaosHarness::ChaosHarness(HarnessOptions options)
    : options_(std::move(options)) {
  if ((options_.node_count != 3U && options_.node_count != 5U) ||
      options_.client_count == 0U || options_.client_count > 256U ||
      options_.duration < std::chrono::seconds(1) ||
      options_.duration > std::chrono::hours(1) ||
      options_.action_interval < std::chrono::milliseconds(50) ||
      options_.server_path.empty() || options_.artifact_directory.empty() ||
      options_.overall_timeout <= options_.duration ||
      options_.request_timeout < std::chrono::milliseconds(50) ||
      options_.request_timeout > std::chrono::seconds(10) ||
      (!options_.enable_chaos && !options_.script.empty())) {
    throw std::invalid_argument("invalid chaos harness options");
  }
  std::ranges::sort(options_.script, {}, &ChaosAction::planned_offset_us);
}

HarnessResult ChaosHarness::run() {
  HarnessResult result;
  result.summary.seed = options_.seed;
  result.artifact_directory = options_.artifact_directory;
  const auto run_start = Clock::now();
  const auto interrupted = [&] {
    return options_.interrupted && options_.interrupted();
  };
  const auto overall_deadline = run_start + options_.overall_timeout;
  ArtifactWriter artifacts(options_.artifact_directory, ArtifactLimits{});
  const auto config_text =
      "{\"version\":1,\"nodes\":" + std::to_string(options_.node_count) +
      ",\"clients\":" + std::to_string(options_.client_count) +
      ",\"duration_ms\":" + std::to_string(options_.duration.count()) +
      ",\"action_interval_ms\":" +
      std::to_string(options_.action_interval.count()) + ",\"seed\":" +
      std::to_string(options_.seed) + ",\"chaos_enabled\":" +
      (options_.enable_chaos ? "true" : "false") +
      ",\"request_timeout_ms\":" +
      std::to_string(options_.request_timeout.count()) + "}\n";
  if (const auto status = artifacts.publish("config.json", config_text);
      !status.ok()) {
    result.diagnostic = status.error;
    return result;
  }
  if (const auto status =
          artifacts.publish("seed.txt", std::to_string(options_.seed) + "\n");
      !status.ok()) {
    result.diagnostic = status.error;
    return result;
  }
  const auto replay_command =
      "forgekv-chaos --replay " +
      shell_quote(std::filesystem::absolute(options_.artifact_directory /
                                            "timeline.jsonl")
                      .string()) +
      " --server " +
      shell_quote(std::filesystem::absolute(options_.server_path).string()) +
      " --artifacts " +
      shell_quote(std::filesystem::absolute(options_.artifact_directory /
                                            "replay-run")
                      .string()) +
      " --keep-success\n";
  if (const auto status = artifacts.publish("replay.txt", replay_command);
      !status.ok()) {
    result.diagnostic = status.error;
    return result;
  }

  try {
    ProcessCluster cluster(ProcessClusterConfig{
        .node_count = options_.node_count,
        .cluster_id = options_.seed == 0U ? 1U : options_.seed,
        .server_path = options_.server_path,
        .root_directory = options_.artifact_directory,
        .proxy_seed = options_.seed,
        .enable_proxies = options_.enable_chaos});
    if (const auto prepare_status = cluster.prepare(); !prepare_status.ok()) {
      result.diagnostic = prepare_status.error;
    } else if (const auto start_status = cluster.start_all();
               !start_status.ok()) {
      result.diagnostic = start_status.error;
    }
    if (result.ok()) {
      std::string children;
      for (std::size_t index = 1U; index <= options_.node_count; ++index) {
        children += std::to_string(cluster.process_id(index)) + "\n";
      }
      if (const auto status = artifacts.publish("children.txt", children);
          !status.ok()) {
        result.diagnostic = status.error;
      }
    }
    if (result.ok()) {
      const auto initial = wait_for_convergence(
          admin_endpoints(cluster, options_.node_count),
          std::min(overall_deadline, Clock::now() + std::chrono::seconds(15)),
          interrupted, options_.request_timeout);
      if (!initial.ok()) {
        result.diagnostic = "initial convergence: " + initial.error;
      } else {
        const auto leader = leader_from(initial.views);
        if (!leader.has_value()) {
          result.diagnostic = "initial convergence omitted leader";
        } else {
          std::vector<std::unique_ptr<ClientState>> clients;
          clients.reserve(options_.client_count);
          for (std::size_t index = 0U; index < options_.client_count; ++index) {
            clients.push_back(std::make_unique<ClientState>(
                make_client_id(options_.seed, index),
                "chaos-client-" + std::to_string(index + 1U)));
          }
          std::atomic<bool> stop_clients{false};
          std::atomic<bool> client_failed{false};
          std::atomic<std::uint64_t> attempts{0U};
          std::atomic<std::uint64_t> acknowledged{0U};
          std::mutex failure_mutex;
          std::string client_failure;
          std::vector<std::jthread> workers;
          workers.reserve(clients.size());
          for (std::size_t index = 0U; index < clients.size(); ++index) {
            workers.emplace_back([&, index, initial_leader = *leader](
                                     const std::stop_token stop_token) {
              try {
                auto endpoint = endpoint_for(cluster, initial_leader);
                std::size_t endpoint_index = index % options_.node_count;
                std::uint64_t operation = 0U;
                while (!stop_token.stop_requested() &&
                       !stop_clients.load(std::memory_order_acquire)) {
                  auto& state = *clients[index];
                  if (!state.next_attempt().has_value()) {
                    switch (operation++ % 4U) {
                      case 0U:
                      case 2U:
                        static_cast<void>(state.begin_put(
                            "value-" + std::to_string(index + 1U) + "-" +
                            std::to_string(operation)));
                        break;
                      case 1U:
                        static_cast<void>(state.begin_get());
                        break;
                      default:
                        static_cast<void>(state.begin_delete());
                        break;
                    }
                  }
                  const auto request = *state.next_attempt();
                  const auto start = elapsed_us(run_start);
                  const auto response = execute_attempt(
                      endpoint, request, options_.request_timeout);
                  const auto finish = elapsed_us(run_start);
                  attempts.fetch_add(1U, std::memory_order_relaxed);
                  const auto recorded = artifacts.append_attempt(AttemptRecord{
                      .client = "client-" + std::to_string(index + 1U),
                      .request = request,
                      .endpoint = endpoint,
                      .result = response.kind,
                      .diagnostic = response.diagnostic,
                      .observed_start_us = start,
                      .observed_finish_us = finish});
                  if (!recorded.ok()) {
                    throw std::runtime_error(recorded.error);
                  }
                  if (response.redirect.has_value()) {
                    endpoint = *response.redirect;
                  } else if (response.kind == AttemptKind::timeout ||
                             response.kind == AttemptKind::transport_error ||
                             response.kind == AttemptKind::protocol_error) {
                    endpoint_index =
                        (endpoint_index + 1U) % options_.node_count;
                    endpoint = endpoint_for(
                        cluster, static_cast<std::uint64_t>(endpoint_index + 1U));
                  }
                  const auto observed = state.observe(request, response);
                  if (observed.completed && observed.success &&
                      request.operation != ClientOperation::get) {
                    acknowledged.fetch_add(1U, std::memory_order_relaxed);
                  }
                  if (observed.completed && !observed.success &&
                      request.operation == ClientOperation::get) {
                    throw std::runtime_error(observed.diagnostic);
                  }
                  std::this_thread::sleep_for(std::chrono::milliseconds(2));
                }
              } catch (const std::exception& error) {
                {
                  std::lock_guard lock(failure_mutex);
                  if (client_failure.empty()) {
                    client_failure = error.what();
                  }
                }
                client_failed.store(true, std::memory_order_release);
                stop_clients.store(true, std::memory_order_release);
              }
            });
          }

          ChaosScheduler scheduler(options_.seed, SchedulerLimits{});
          const auto chaos_start = Clock::now();
          std::size_t script_index = 0U;
          std::uint64_t last_dead = 0U;
          std::set<std::uint64_t> intentionally_dead;
          auto next_action = chaos_start;
          auto next_artifact_check = chaos_start;
          while (Clock::now() - chaos_start < options_.duration &&
                 Clock::now() < overall_deadline &&
                 !interrupted() &&
                 !client_failed.load(std::memory_order_acquire)) {
            if (Clock::now() >= next_artifact_check) {
              if (const auto status =
                      validate_artifact_bounds(options_.artifact_directory);
                  !status.ok()) {
                result.diagnostic = status.error;
                break;
              }
              next_artifact_check = Clock::now() + std::chrono::seconds(1);
            }
            ChaosAction requested;
            bool due = false;
            if (!options_.script.empty()) {
              if (script_index < options_.script.size()) {
                requested = options_.script[script_index];
                due = elapsed_us(chaos_start) >= requested.planned_offset_us;
              }
            } else if (options_.enable_chaos && Clock::now() >= next_action) {
              requested = scheduler.next(
                  static_cast<std::uint64_t>(
                      std::chrono::duration_cast<std::chrono::microseconds>(
                          next_action - chaos_start)
                          .count()),
                  current_cluster_view(cluster, options_.node_count,
                                       options_.request_timeout));
              due = true;
              next_action += options_.action_interval;
            }
            if (!due) {
              std::this_thread::sleep_for(std::chrono::milliseconds(5));
              continue;
            }
            auto view = current_cluster_view(cluster, options_.node_count,
                                             options_.request_timeout);
            for (const auto& node : view.nodes) {
              if (node.state == NodeState::dead &&
                  !intentionally_dead.contains(node.id)) {
                result.diagnostic = "unexpected child exit: node " +
                                    std::to_string(node.id);
                break;
              }
            }
            if (!result.ok()) break;
            if (requested.kind == ActionKind::restart_node &&
                requested.node == 0U) {
              requested.node = last_dead != 0U
                                   ? last_dead
                                   : first_node_with_state(view, NodeState::dead);
            }
            const auto action = scheduler.normalize(requested, view);
            const auto action_start = elapsed_us(run_start);
            const auto action_status =
                apply_action(cluster, action, options_.node_count);
            const auto action_finish = elapsed_us(run_start);
            if (action.kind == ActionKind::kill_leader ||
                action.kind == ActionKind::kill_follower) {
              last_dead = action.node;
              if (action_status.ok()) intentionally_dead.insert(action.node);
            } else if (action.kind == ActionKind::restart_node &&
                       action_status.ok()) {
              intentionally_dead.erase(action.node);
            }
            const auto recorded =
                artifacts.append_action(action, action_start, action_finish);
            if (!recorded.ok() || !action_status.ok()) {
              result.diagnostic = !recorded.ok() ? recorded.error
                                                 : action_status.error;
              break;
            }
            ++result.summary.actions;
            ++script_index;
            if (const auto status =
                    validate_artifact_bounds(options_.artifact_directory);
                !status.ok()) {
              result.diagnostic = status.error;
              break;
            }
          }

          stop_clients.store(true, std::memory_order_release);
          for (auto& worker : workers) {
            worker.request_stop();
          }
          workers.clear();
          result.summary.attempts = attempts.load(std::memory_order_relaxed);
          result.summary.acknowledged_writes =
              acknowledged.load(std::memory_order_relaxed);
          if (result.ok() && client_failed.load(std::memory_order_acquire)) {
            std::lock_guard lock(failure_mutex);
            result.diagnostic = "client invariant: " + client_failure;
          }

          if (result.ok() && interrupted()) {
            result.diagnostic = "campaign interrupted by signal";
          }

          cluster.heal_network();
          auto state = cluster.refresh();
          for (const auto& node : state.nodes) {
            if (node.state == NodeState::paused) {
              static_cast<void>(cluster.resume_node(node.id));
            } else if (node.state == NodeState::dead) {
              if (!intentionally_dead.contains(node.id) && result.ok()) {
                result.diagnostic = "unexpected child exit: node " +
                                    std::to_string(node.id);
              }
              const auto restart_status = cluster.restart_node(node.id);
              if (!restart_status.ok() && result.ok()) {
                result.diagnostic = "cleanup restart: " + restart_status.error;
              }
            }
          }

          ConvergenceResult converged;
          if (result.ok()) {
            converged = wait_for_convergence(
                admin_endpoints(cluster, options_.node_count),
                std::min(overall_deadline,
                         Clock::now() + std::chrono::seconds(15)),
                interrupted, options_.request_timeout);
            if (!converged.ok()) {
              result.diagnostic = "post-chaos convergence: " + converged.error;
            }
          }
          if (result.ok() && converged.ok()) {
            auto leader_node = leader_from(converged.views);
            if (!leader_node.has_value()) {
              result.diagnostic = "post-chaos leader unavailable";
            } else {
              for (std::size_t index = 0U; index < clients.size(); ++index) {
                auto& client = *clients[index];
                const auto resolve_deadline =
                    std::min(overall_deadline,
                             Clock::now() + std::chrono::seconds(5));
                auto endpoint = endpoint_for(cluster, *leader_node);
                while (client.next_attempt().has_value() &&
                       Clock::now() < resolve_deadline) {
                  const auto request = *client.next_attempt();
                  const auto start = elapsed_us(run_start);
                  const auto response = execute_attempt(
                      endpoint, request, options_.request_timeout);
                  const auto finish = elapsed_us(run_start);
                  ++result.summary.attempts;
                  const auto recorded = artifacts.append_attempt(AttemptRecord{
                      .client = "client-" + std::to_string(index + 1U),
                      .request = request,
                      .endpoint = endpoint,
                      .result = response.kind,
                      .diagnostic = response.diagnostic,
                      .observed_start_us = start,
                      .observed_finish_us = finish});
                  if (!recorded.ok()) {
                    result.diagnostic = recorded.error;
                    break;
                  }
                  if (response.redirect.has_value()) {
                    endpoint = *response.redirect;
                  }
                  const auto observed = client.observe(request, response);
                  if (observed.completed && observed.success &&
                      request.operation != ClientOperation::get) {
                    ++result.summary.acknowledged_writes;
                  }
                  if (observed.completed && !observed.success &&
                      request.operation == ClientOperation::get) {
                    result.diagnostic = observed.diagnostic;
                    break;
                  }
                }
                if (result.ok() && client.next_attempt().has_value()) {
                  result.diagnostic = "ambiguous request did not resolve";
                }
                if (!result.ok()) {
                  break;
                }
              }
            }
          }

          if (result.ok()) {
            converged = wait_for_convergence(
                admin_endpoints(cluster, options_.node_count),
                std::min(overall_deadline,
                         Clock::now() + std::chrono::seconds(15)),
                interrupted, options_.request_timeout);
            if (!converged.ok()) {
              result.diagnostic = "resolved-state convergence: " +
                                  converged.error;
            }
          }
          if (result.ok()) {
            if (const auto stop_status = cluster.stop_all();
                !stop_status.ok()) {
              result.diagnostic = "durable stop: " + stop_status.error;
            } else if (const auto status = cluster.start_all(); !status.ok()) {
              result.diagnostic = "durable restart: " + status.error;
            }
          }
          if (result.ok()) {
            converged = wait_for_convergence(
                admin_endpoints(cluster, options_.node_count),
                std::min(overall_deadline,
                         Clock::now() + std::chrono::seconds(20)),
                interrupted, options_.request_timeout);
            if (!converged.ok()) {
              result.diagnostic = "restart convergence: " + converged.error;
            } else {
              result.summary.restart_verified = true;
            }
          }
          if (result.ok()) {
            const auto leader_node = leader_from(converged.views);
            if (!leader_node.has_value()) {
              result.diagnostic = "verification leader unavailable";
            } else {
              for (std::size_t index = 0U; index < clients.size(); ++index) {
                auto& client = *clients[index];
                const auto request = client.begin_get();
                auto endpoint = endpoint_for(cluster, *leader_node);
                const auto verify_deadline =
                    std::min(overall_deadline,
                             Clock::now() + std::chrono::seconds(5));
                while (Clock::now() < verify_deadline) {
                  const auto start = elapsed_us(run_start);
                  const auto response = execute_attempt(
                      endpoint, request, options_.request_timeout);
                  const auto finish = elapsed_us(run_start);
                  ++result.summary.attempts;
                  const auto recorded = artifacts.append_attempt(AttemptRecord{
                      .client = "client-" + std::to_string(index + 1U),
                      .request = request,
                      .endpoint = endpoint,
                      .result = response.kind,
                      .diagnostic = response.diagnostic,
                      .observed_start_us = start,
                      .observed_finish_us = finish});
                  if (!recorded.ok()) {
                    result.diagnostic = recorded.error;
                    break;
                  }
                  if (response.redirect.has_value()) {
                    endpoint = *response.redirect;
                  }
                  const auto observed = client.observe(request, response);
                  if (observed.completed) {
                    if (!observed.success) {
                      result.diagnostic = observed.diagnostic;
                    }
                    break;
                  }
                }
                if (result.ok() && client.next_attempt().has_value()) {
                  result.diagnostic = "final key verification timed out";
                }
                if (!result.ok()) {
                  break;
                }
              }
            }
          }
          result.summary.converged = result.ok();
        }
      }
    }

    const auto failure_before_metrics = result.diagnostic;
    try {
      ArtifactWriter metrics(options_.artifact_directory / "metrics",
                             ArtifactLimits{});
      for (const auto endpoint : admin_endpoints(cluster, options_.node_count)) {
        const auto snapshot = fetch_admin_text_until(
            endpoint, "/metrics",
            std::min(overall_deadline, Clock::now() + std::chrono::seconds(5)),
            std::max(std::chrono::milliseconds(500), options_.request_timeout),
            interrupted);
        if ((!snapshot.ok() || snapshot.status != 200) && result.ok()) {
          result.diagnostic = "metrics snapshot failed for node " +
                              std::to_string(endpoint.node) + ": " +
                              snapshot.error;
        }
        const auto contents = snapshot.ok() && snapshot.status == 200
                                  ? snapshot.body
                                  : "# collection_error " + snapshot.error + "\n";
        const auto status = metrics.publish(
            "node-" + std::to_string(endpoint.node) + ".prom", contents);
        if (!status.ok()) {
          if (result.ok()) {
            result.diagnostic = "metrics artifact: " + status.error;
          } else {
            result.diagnostic += "; metrics artifact: " + status.error;
          }
        }
      }
    } catch (const std::exception& error) {
      if (result.ok()) {
        result.diagnostic = "metrics artifact: " + std::string(error.what());
      } else {
        result.diagnostic += "; metrics artifact: " + std::string(error.what());
      }
    }
    result.summary.converged = result.ok() && result.summary.converged;
    if (!result.ok()) {
      const auto& first = failure_before_metrics.empty()
                              ? result.diagnostic
                              : failure_before_metrics;
      result.summary.failure_category = failure_category(first);
      result.summary.first_evidence = first;
    }
    const auto cleanup_failure = [&](const std::string& detail) {
      if (result.ok()) {
        result.diagnostic = detail;
        result.summary.failure_category = failure_category(detail);
        result.summary.first_evidence = detail;
      } else {
        result.diagnostic += "; cleanup: " + detail;
      }
      result.summary.converged = false;
    };
    if (const auto status = cluster.stop_all(); !status.ok()) {
      cleanup_failure("final stop: " + status.error);
    }
    if (const auto status = scan_critical_logs(options_.artifact_directory);
        !status.ok()) {
      cleanup_failure(status.error);
    }
    if (const auto status = validate_artifact_bounds(options_.artifact_directory);
        !status.ok()) {
      cleanup_failure(status.error);
    }
  } catch (const std::exception& error) {
    result.diagnostic = error.what();
  }

  if (!result.ok() && result.summary.failure_category.empty()) {
    result.summary.failure_category = failure_category(result.diagnostic);
    result.summary.first_evidence = result.diagnostic;
    result.summary.converged = false;
  }

  const auto summary =
      "{\"version\":1,\"result\":" +
      json_string(result.ok() ? "pass" : "fail") + ",\"seed\":" +
      std::to_string(result.summary.seed) + ",\"attempts\":" +
      std::to_string(result.summary.attempts) +
      ",\"acknowledged_writes\":" +
      std::to_string(result.summary.acknowledged_writes) +
      ",\"actions\":" + std::to_string(result.summary.actions) +
      ",\"chaos_enabled\":" +
      (options_.enable_chaos ? "true" : "false") +
      ",\"converged\":" +
      std::string(result.summary.converged ? "true" : "false") +
      ",\"restart_verified\":" +
      std::string(result.summary.restart_verified ? "true" : "false") +
      ",\"failure_category\":" +
      json_string(result.summary.failure_category) +
      ",\"first_evidence\":" +
      json_string(result.summary.first_evidence) +
      ",\"diagnostic\":" + json_string(result.diagnostic) + "}\n";
  if (const auto status = artifacts.publish("summary.json", summary);
      !status.ok()) {
    if (result.ok()) {
      result.diagnostic = status.error;
      result.summary.failure_category = "artifact I/O";
      result.summary.first_evidence = status.error;
      result.summary.converged = false;
    } else {
      result.diagnostic += "; summary artifact I/O: " + status.error;
    }
  }
  return result;
}

}  // namespace forgekv::chaos
