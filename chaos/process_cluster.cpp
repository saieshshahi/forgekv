#include "chaos/process_cluster.h"

#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <fcntl.h>
#include <netinet/in.h>
#include <set>
#include <sstream>
#include <stdexcept>
#include <sys/socket.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace forgekv::chaos {
namespace {

std::uint16_t reserve_port(std::set<std::uint16_t>& used) noexcept {
  for (std::size_t attempt = 0; attempt < 1000U; ++attempt) {
    const int descriptor = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (descriptor < 0) {
      return 0U;
    }
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    const bool bound =
        ::bind(descriptor, reinterpret_cast<sockaddr*>(&address),
               sizeof(address)) == 0;
    socklen_t size = sizeof(address);
    const bool named =
        bound && ::getsockname(descriptor, reinterpret_cast<sockaddr*>(&address),
                               &size) == 0;
    static_cast<void>(::close(descriptor));
    if (named) {
      const auto port = ntohs(address.sin_port);
      if (port != 0U && used.insert(port).second) {
        return port;
      }
    }
  }
  return 0U;
}

std::string signal_error(const char* action) {
  return std::string(action) + ": errno=" + std::to_string(errno);
}

}  // namespace

class ProcessCluster::Impl final {
 public:
  explicit Impl(ProcessClusterConfig config) : config_(std::move(config)) {
    if ((config_.node_count != 1U && config_.node_count != 3U &&
         config_.node_count != 5U) ||
        config_.cluster_id == 0U || config_.server_path.empty() ||
        config_.root_directory.empty() || config_.shutdown_grace.count() <= 0) {
      throw std::invalid_argument("invalid process cluster configuration");
    }
  }

  ~Impl() {
    static_cast<void>(stop_all());
    proxies_.clear();
  }

  ClusterStatus prepare() {
    if (prepared_) {
      return {.error = "process cluster is already prepared"};
    }
    std::error_code error;
    std::filesystem::create_directories(config_.root_directory / "logs", error);
    if (error) {
      return {.error = "create cluster directory: " + error.message()};
    }
    nodes_.resize(config_.node_count);
    std::set<std::uint16_t> used;
    for (std::size_t index = 0U; index < nodes_.size(); ++index) {
      auto& node = nodes_[index];
      node.id = index + 1U;
      node.client_port = reserve_port(used);
      node.peer_port = reserve_port(used);
      node.admin_port = reserve_port(used);
      node.data_directory =
          config_.root_directory / "data" /
          ("node-" + std::to_string(node.id));
      node.log_path = config_.root_directory / "logs" /
                      ("node-" + std::to_string(node.id) + ".log");
      std::filesystem::create_directories(node.data_directory, error);
      if (node.client_port == 0U || node.peer_port == 0U ||
          node.admin_port == 0U || error) {
        return {.error = "reserve node resources"};
      }
    }
    proxy_ports_.assign(config_.node_count * config_.node_count, 0U);
    for (std::size_t source = 0U;
         config_.enable_proxies && source < nodes_.size(); ++source) {
      for (std::size_t destination = 0U; destination < nodes_.size();
           ++destination) {
        if (source == destination) {
          continue;
        }
        const auto port = reserve_port(used);
        if (port == 0U) {
          proxies_.clear();
          return {.error = "reserve proxy port"};
        }
        auto proxy = std::make_unique<FaultProxy>(FaultProxyConfig{
            .bind_port = port,
            .destination_port = nodes_[destination].peer_port,
            .seed = config_.proxy_seed ^
                    (static_cast<std::uint64_t>(source + 1U) << 32U) ^
                    static_cast<std::uint64_t>(destination + 1U)});
        const auto status = proxy->start();
        if (!status.ok()) {
          proxies_.clear();
          return {.error = "start proxy: " + status.error};
        }
        proxy_ports_[key(source, destination)] = proxy->port();
        proxies_.push_back(ProxyRecord{.source = source,
                                       .destination = destination,
                                       .proxy = std::move(proxy)});
      }
    }
    prepared_ = true;
    return {};
  }

  ClusterStatus start_all() {
    if (!prepared_) {
      return {.error = "process cluster is not prepared"};
    }
    for (auto& node : nodes_) {
      if (const auto status = start_node(node.id); !status.ok()) {
        static_cast<void>(stop_all());
        return status;
      }
    }
    return {};
  }

  ClusterStatus start_node(const std::uint64_t node_id) {
    auto* node = find_node(node_id);
    if (!prepared_ || node == nullptr) {
      return {.error = "unknown or unprepared node"};
    }
    refresh_node(*node);
    if (node->state != NodeState::dead || node->pid > 0) {
      return {.error = "node is already live"};
    }
    auto arguments = argument_vector(node_id);
    std::vector<char*> argv;
    argv.reserve(arguments.size() + 2U);
    const auto executable = config_.server_path.string();
    argv.push_back(const_cast<char*>(executable.c_str()));
    for (auto& argument : arguments) {
      argv.push_back(argument.data());
    }
    argv.push_back(nullptr);
    const auto child = ::fork();
    if (child < 0) {
      return {.error = signal_error("fork")};
    }
    if (child == 0) {
      const int log = ::open(node->log_path.c_str(),
                             O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
      if (log >= 0) {
        static_cast<void>(::dup2(log, STDOUT_FILENO));
        static_cast<void>(::dup2(log, STDERR_FILENO));
        if (log != STDOUT_FILENO && log != STDERR_FILENO) {
          static_cast<void>(::close(log));
        }
      }
      ::execv(executable.c_str(), argv.data());
      ::_exit(126);
    }
    node->pid = child;
    node->state = NodeState::running;
    return {};
  }

  ClusterStatus restart_node(const std::uint64_t node_id) {
    const auto* node = find_node(node_id);
    if (node == nullptr || node->state != NodeState::dead) {
      return {.error = "only a dead node can be restarted"};
    }
    return start_node(node_id);
  }

  ClusterStatus kill_node(const std::uint64_t node_id) {
    auto* node = find_node(node_id);
    if (node == nullptr || node->state == NodeState::dead || node->pid <= 0) {
      return {.error = "node is not live"};
    }
    if (::kill(node->pid, SIGKILL) != 0 && errno != ESRCH) {
      return {.error = signal_error("kill")};
    }
    reap_blocking(*node);
    return {};
  }

  ClusterStatus pause_node(const std::uint64_t node_id) {
    auto* node = find_node(node_id);
    if (node == nullptr || node->state != NodeState::running || node->pid <= 0) {
      return {.error = "only a running node can be paused"};
    }
    if (::kill(node->pid, SIGSTOP) != 0) {
      return {.error = signal_error("pause")};
    }
    node->state = NodeState::paused;
    return {};
  }

  ClusterStatus resume_node(const std::uint64_t node_id) {
    auto* node = find_node(node_id);
    if (node == nullptr || node->state != NodeState::paused || node->pid <= 0) {
      return {.error = "only a paused node can be resumed"};
    }
    if (::kill(node->pid, SIGCONT) != 0) {
      return {.error = signal_error("resume")};
    }
    node->state = NodeState::running;
    return {};
  }

  ClusterStatus stop_all() noexcept {
    for (auto& node : nodes_) {
      stop_node(node);
      if (node.pid > 0 || node.state != NodeState::dead) {
        return {.error = "failed to stop and reap node " +
                         std::to_string(node.id)};
      }
    }
    return {};
  }

  ClusterView refresh() {
    ClusterView view;
    view.nodes.reserve(nodes_.size());
    for (auto& node : nodes_) {
      refresh_node(node);
      view.nodes.push_back(NodeView{.id = node.id, .state = node.state});
    }
    return view;
  }

  NodeState state(const std::uint64_t node_id) const noexcept {
    const auto* node = find_node(node_id);
    return node == nullptr ? NodeState::dead : node->state;
  }

  std::uint16_t client_port(const std::uint64_t node_id) const noexcept {
    const auto* node = find_node(node_id);
    return node == nullptr ? 0U : node->client_port;
  }

  std::uint16_t peer_port(const std::uint64_t node_id) const noexcept {
    const auto* node = find_node(node_id);
    return node == nullptr ? 0U : node->peer_port;
  }

  std::uint16_t admin_port(const std::uint64_t node_id) const noexcept {
    const auto* node = find_node(node_id);
    return node == nullptr ? 0U : node->admin_port;
  }

  std::int64_t process_id(const std::uint64_t node_id) const noexcept {
    const auto* node = find_node(node_id);
    return node == nullptr ? -1 : static_cast<std::int64_t>(node->pid);
  }

  std::uint16_t proxy_port(const std::uint64_t source,
                           const std::uint64_t destination) const noexcept {
    if (source == 0U || destination == 0U || source > nodes_.size() ||
        destination > nodes_.size() || source == destination ||
        proxy_ports_.empty()) {
      return 0U;
    }
    return proxy_ports_[key(static_cast<std::size_t>(source - 1U),
                            static_cast<std::size_t>(destination - 1U))];
  }

  std::string server_arguments(const std::uint64_t node_id) const {
    const auto arguments = argument_vector(node_id);
    std::ostringstream output;
    for (std::size_t index = 0U; index < arguments.size(); ++index) {
      if (index != 0U) {
        output << ' ';
      }
      output << arguments[index];
    }
    return output.str();
  }

  ClusterStatus set_link_policy(const std::uint64_t source,
                                const std::uint64_t destination,
                                const LinkPolicy policy) {
    auto* proxy = find_proxy(source, destination);
    if (proxy == nullptr) {
      return {.error = "unknown directed link"};
    }
    const auto status = proxy->set_policy(policy);
    return status.ok() ? ClusterStatus{}
                       : ClusterStatus{.error = status.error};
  }

  void heal_network() noexcept {
    for (auto& proxy : proxies_) {
      static_cast<void>(proxy.proxy->set_policy(LinkPolicy{}));
    }
  }

 private:
  struct NodeProcess final {
    std::uint64_t id{};
    std::uint16_t client_port{};
    std::uint16_t peer_port{};
    std::uint16_t admin_port{};
    std::filesystem::path data_directory;
    std::filesystem::path log_path;
    pid_t pid{-1};
    NodeState state{NodeState::dead};
  };

  struct ProxyRecord final {
    std::size_t source{};
    std::size_t destination{};
    std::unique_ptr<FaultProxy> proxy;
  };

  std::size_t key(const std::size_t source,
                  const std::size_t destination) const noexcept {
    return source * config_.node_count + destination;
  }

  NodeProcess* find_node(const std::uint64_t id) noexcept {
    return id == 0U || id > nodes_.size() ? nullptr
                                          : &nodes_[static_cast<std::size_t>(id - 1U)];
  }

  const NodeProcess* find_node(const std::uint64_t id) const noexcept {
    return id == 0U || id > nodes_.size() ? nullptr
                                          : &nodes_[static_cast<std::size_t>(id - 1U)];
  }

  FaultProxy* find_proxy(const std::uint64_t source,
                         const std::uint64_t destination) noexcept {
    if (source == 0U || destination == 0U) {
      return nullptr;
    }
    for (auto& proxy : proxies_) {
      if (proxy.source + 1U == source &&
          proxy.destination + 1U == destination) {
        return proxy.proxy.get();
      }
    }
    return nullptr;
  }

  std::vector<std::string> argument_vector(const std::uint64_t node_id) const {
    const auto* node = find_node(node_id);
    if (node == nullptr) {
      return {};
    }
    std::vector<std::string> result{
        "--cluster-id", std::to_string(config_.cluster_id),
        "--node-id", std::to_string(node_id),
        "--data-dir", node->data_directory.string(),
        "--client-port", std::to_string(node->client_port),
        "--peer-port", std::to_string(node->peer_port),
        "--admin-port", std::to_string(node->admin_port),
        "--client-timeout-ms", "1000",
    };
    for (const auto& peer : nodes_) {
      const auto route = peer.id == node_id || !config_.enable_proxies
                             ? peer.peer_port
                             : proxy_port(node_id, peer.id);
      result.push_back("--peer");
      result.push_back(std::to_string(peer.id) + "=127.0.0.1:" +
                       std::to_string(route) + ":" +
                       std::to_string(peer.client_port));
    }
    return result;
  }

  void refresh_node(NodeProcess& node) noexcept {
    if (node.pid <= 0) {
      node.state = NodeState::dead;
      return;
    }
    int status = 0;
    const auto result = ::waitpid(node.pid, &status, WNOHANG);
    if (result == node.pid || (result < 0 && errno == ECHILD)) {
      node.pid = -1;
      node.state = NodeState::dead;
    }
  }

  void reap_blocking(NodeProcess& node) noexcept {
    if (node.pid <= 0) {
      node.state = NodeState::dead;
      return;
    }
    int status = 0;
    while (::waitpid(node.pid, &status, 0) < 0 && errno == EINTR) {
    }
    node.pid = -1;
    node.state = NodeState::dead;
  }

  void stop_node(NodeProcess& node) noexcept {
    refresh_node(node);
    if (node.pid <= 0) {
      return;
    }
    if (node.state == NodeState::paused) {
      static_cast<void>(::kill(node.pid, SIGCONT));
      node.state = NodeState::running;
    }
    static_cast<void>(::kill(node.pid, SIGTERM));
    const auto deadline = std::chrono::steady_clock::now() +
                          config_.shutdown_grace;
    while (std::chrono::steady_clock::now() < deadline) {
      refresh_node(node);
      if (node.pid <= 0) {
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    static_cast<void>(::kill(node.pid, SIGKILL));
    reap_blocking(node);
  }

  ProcessClusterConfig config_;
  bool prepared_{};
  std::vector<NodeProcess> nodes_;
  std::vector<std::uint16_t> proxy_ports_;
  std::vector<ProxyRecord> proxies_;
};

ProcessCluster::ProcessCluster(ProcessClusterConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

ProcessCluster::~ProcessCluster() = default;

ClusterStatus ProcessCluster::prepare() { return impl_->prepare(); }
ClusterStatus ProcessCluster::start_all() { return impl_->start_all(); }
ClusterStatus ProcessCluster::start_node(const std::uint64_t node) {
  return impl_->start_node(node);
}
ClusterStatus ProcessCluster::restart_node(const std::uint64_t node) {
  return impl_->restart_node(node);
}
ClusterStatus ProcessCluster::kill_node(const std::uint64_t node) {
  return impl_->kill_node(node);
}
ClusterStatus ProcessCluster::pause_node(const std::uint64_t node) {
  return impl_->pause_node(node);
}
ClusterStatus ProcessCluster::resume_node(const std::uint64_t node) {
  return impl_->resume_node(node);
}
ClusterStatus ProcessCluster::stop_all() noexcept { return impl_->stop_all(); }
ClusterView ProcessCluster::refresh() { return impl_->refresh(); }
NodeState ProcessCluster::state(const std::uint64_t node) const noexcept {
  return impl_->state(node);
}
std::uint16_t ProcessCluster::client_port(const std::uint64_t node) const noexcept {
  return impl_->client_port(node);
}
std::uint16_t ProcessCluster::peer_port(const std::uint64_t node) const noexcept {
  return impl_->peer_port(node);
}
std::uint16_t ProcessCluster::admin_port(const std::uint64_t node) const noexcept {
  return impl_->admin_port(node);
}
std::int64_t ProcessCluster::process_id(const std::uint64_t node) const noexcept {
  return impl_->process_id(node);
}
std::uint16_t ProcessCluster::proxy_port(
    const std::uint64_t source, const std::uint64_t destination) const noexcept {
  return impl_->proxy_port(source, destination);
}
std::string ProcessCluster::server_arguments(const std::uint64_t node) const {
  return impl_->server_arguments(node);
}
ClusterStatus ProcessCluster::set_link_policy(const std::uint64_t source,
                                              const std::uint64_t destination,
                                              const LinkPolicy policy) {
  return impl_->set_link_policy(source, destination, policy);
}
void ProcessCluster::heal_network() noexcept { impl_->heal_network(); }

}  // namespace forgekv::chaos
