#include "cluster/node.h"
#include "common/logging.h"

#include <chrono>
#include <csignal>
#include <cstdint>
#include <exception>
#include <iostream>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

volatile std::sig_atomic_t stop_requested = 0;
volatile std::sig_atomic_t peer_toggle_requested = 0;

void request_stop(const int) { stop_requested = 1; }
void request_peer_toggle(const int) { peer_toggle_requested = 1; }

std::uint64_t parse_u64(const std::string& text, const char* field) {
  std::size_t consumed = 0;
  const auto value = std::stoull(text, &consumed);
  if (consumed != text.size()) {
    throw std::invalid_argument(std::string("invalid ") + field);
  }
  return value;
}

std::uint16_t parse_port(const std::string& text, const char* field,
                         const bool allow_zero = false) {
  const auto value = parse_u64(text, field);
  if ((!allow_zero && value == 0) ||
      value > std::numeric_limits<std::uint16_t>::max()) {
    throw std::invalid_argument(std::string("invalid ") + field);
  }
  return static_cast<std::uint16_t>(value);
}

std::uint32_t parse_u32(const std::string& text, const char* field) {
  const auto value = parse_u64(text, field);
  if (value == 0 || value > std::numeric_limits<std::uint32_t>::max()) {
    throw std::invalid_argument(std::string("invalid ") + field);
  }
  return static_cast<std::uint32_t>(value);
}

void usage() {
  std::cerr
      << "usage: forgekv-server --cluster-id ID --node-id ID --data-dir PATH "
         "--client-port PORT --peer-port PORT "
         "--peer ID=HOST:PEER_PORT:CLIENT_PORT [--peer ...] "
         "(bracket IPv6 hosts) "
         "[--bind ADDRESS] [--admin-bind ADDRESS] [--admin-port PORT] "
         "[--client-timeout-ms MILLISECONDS] "
         "[--max-pending-reads COUNT] [--snapshot-threshold ENTRIES]\n";
}

}  // namespace

int main(const int argc, char** argv) {
  try {
    forgekv::cluster::ClusterNodeConfig config;
    for (int index = 1; index < argc; ++index) {
      const std::string_view argument(argv[index]);
      if (index + 1 >= argc) {
        usage();
        return 2;
      }
      const std::string value(argv[++index]);
      if (argument == "--cluster-id") {
        config.cluster_id = parse_u64(value, "cluster ID");
      } else if (argument == "--node-id") {
        config.node_id = parse_u64(value, "node ID");
      } else if (argument == "--data-dir") {
        config.data_directory = value;
      } else if (argument == "--client-port") {
        config.client_port = parse_port(value, "client port");
      } else if (argument == "--peer-port") {
        config.peer_port = parse_port(value, "peer port");
      } else if (argument == "--admin-port") {
        config.admin_port = parse_port(value, "admin port", true);
      } else if (argument == "--peer") {
        config.peers.push_back(forgekv::cluster::parse_peer_address(value));
      } else if (argument == "--bind") {
        config.bind_address = value;
      } else if (argument == "--admin-bind") {
        config.admin_bind_address = value;
      } else if (argument == "--client-timeout-ms") {
        config.client_timeout_ms = parse_u32(value, "client timeout");
      } else if (argument == "--max-pending-reads") {
        config.max_pending_reads = parse_u32(value, "maximum pending reads");
      } else if (argument == "--snapshot-threshold") {
        config.snapshot_threshold = parse_u32(value, "snapshot threshold");
      } else {
        usage();
        return 2;
      }
    }

    forgekv::cluster::ClusterNode node(std::move(config));
    if (const auto error = node.start(); error.has_value()) {
      forgekv::common::Logger::write(forgekv::common::Severity::critical,
                                     "server_startup_failed detail=" + *error);
      return 1;
    }
    std::signal(SIGINT, request_stop);
    std::signal(SIGTERM, request_stop);
    std::signal(SIGUSR1, request_peer_toggle);
    std::cout << "READY client_port=" << node.client_port()
              << " peer_port=" << node.peer_port()
              << " admin_port=" << node.admin_port() << std::endl;
    bool peer_traffic_enabled = true;
    while (stop_requested == 0 && !node.failed()) {
      if (peer_toggle_requested != 0) {
        peer_toggle_requested = 0;
        peer_traffic_enabled = !peer_traffic_enabled;
        node.set_peer_traffic_enabled(peer_traffic_enabled);
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    const bool failed = node.failed();
    FORGEKV_LOG(forgekv::common::Severity::info,
                failed ? "server_stopping reason=internal_failure"
                       : "server_stopping reason=signal");
    node.stop();
    return failed ? 1 : 0;
  } catch (const std::exception& error) {
    forgekv::common::Logger::write(forgekv::common::Severity::critical,
                                   std::string("server_fatal detail=") +
                                       error.what());
    return 2;
  }
}
