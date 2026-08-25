#include "raft/persisted_raft_node.h"

#include <cerrno>
#include <csignal>
#include <cstddef>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <string_view>

#include <fcntl.h>
#include <unistd.h>

namespace {

using forgekv::raft::Action;
using forgekv::raft::AppendEntries;
using forgekv::raft::EntryKind;
using forgekv::raft::LogEntry;
using forgekv::raft::PersistedRaftNode;
using forgekv::raft::PersistedRaftOptions;
using forgekv::raft::RaftConfig;
using forgekv::raft::RaftCrashPoint;
using forgekv::raft::RequestVote;
using forgekv::raft::SendMessage;

RaftConfig config() {
  return RaftConfig{
      .self_id = 1,
      .voters = {1, 2, 3},
      .election_timeout_min = 100,
      .election_timeout_max = 200,
      .heartbeat_interval = 25,
      .random_seed = 811,
  };
}

RaftCrashPoint parse_point(const std::string_view text) {
  if (text == "before_persist") {
    return RaftCrashPoint::before_persist;
  }
  if (text == "after_write") {
    return RaftCrashPoint::after_write;
  }
  if (text == "after_file_sync") {
    return RaftCrashPoint::after_file_sync;
  }
  if (text == "after_rename") {
    return RaftCrashPoint::after_rename;
  }
  if (text == "after_sync") {
    return RaftCrashPoint::after_sync;
  }
  if (text == "before_response") {
    return RaftCrashPoint::before_response;
  }
  if (text == "after_response") {
    return RaftCrashPoint::after_response;
  }
  throw std::invalid_argument("unknown crash point");
}

void write_response_marker(const std::filesystem::path& path) {
  const auto descriptor =
      ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (descriptor < 0) {
    throw std::runtime_error("cannot create response marker");
  }
  const char marker = '1';
  if (::write(descriptor, &marker, 1) != 1 || ::fdatasync(descriptor) != 0) {
    static_cast<void>(::close(descriptor));
    throw std::runtime_error("cannot persist response marker");
  }
  static_cast<void>(::close(descriptor));
}

[[noreturn]] void crash_now() {
  static_cast<void>(::kill(::getpid(), SIGKILL));
  ::_exit(125);
}

}  // namespace

int main(const int argc, char** argv) {
  if (argc != 5) {
    return 2;
  }
  const std::filesystem::path directory(argv[1]);
  const std::string_view mode(argv[2]);
  const auto target = parse_point(argv[3]);
  const auto occurrence = static_cast<std::size_t>(std::stoull(argv[4]));
  const auto marker = directory / "response.marker";

  if (mode == "log") {
    auto bootstrap = PersistedRaftNode::open(PersistedRaftOptions{
        .config = config(),
        .data_directory = directory,
        .initial_time = 0,
        .output = [](const Action&) {},
        .crash_hook = {},
    });
    bootstrap.step(2, RequestVote{.term = 2,
                                  .candidate_id = 2,
                                  .last_log_index = 0,
                                  .last_log_term = 0});
  }

  std::size_t observed = 0;
  auto node = PersistedRaftNode::open(PersistedRaftOptions{
      .config = config(),
      .data_directory = directory,
      .initial_time = 50,
      .output = [&marker](const Action& action) {
        if (std::holds_alternative<SendMessage>(action)) {
          write_response_marker(marker);
        }
      },
      .crash_hook = [target, occurrence, &observed](const RaftCrashPoint point) {
        if (point == target && ++observed == occurrence) {
          crash_now();
        }
      },
  });

  if (mode == "recover") {
    crash_now();
  }

  if (mode == "vote") {
    node.step(2, RequestVote{.term = 1,
                             .candidate_id = 2,
                             .last_log_index = 0,
                             .last_log_term = 0});
  } else if (mode == "log" || mode == "higher_log") {
    node.step(2, AppendEntries{
                     .term = 2,
                     .leader_id = 2,
                     .previous_log_index = 0,
                     .previous_log_term = 0,
                     .entries = {LogEntry{
                         .index = 1,
                         .term = 2,
                         .kind = EntryKind::command,
                         .command = {std::byte{0x5A}},
                     }},
                     .leader_commit = 0,
                     .rpc_id = 44,
                 });
  } else {
    return 3;
  }
  return 4;
}
