#pragma once

#include "config/config.hpp"
#include <event2/event.h>
#include <event2/listener.h>
#include <cstddef>
#include <map>
#include <string>
#include <vector>

struct evbuffer;

namespace pgpooler {
namespace server {

/** Runs the dispatcher loop: accept TCP, read first packet, resolve, send fd to worker. Does not return until event_base stops. */
void run_dispatcher(
    struct event_base* base,
    const std::string& listen_host,
    std::uint16_t listen_port,
    const std::vector<int>& worker_socket_fds,
    const std::map<std::string, std::size_t>& backend_to_worker,
    pgpooler::config::BackendResolver resolver);

/** Runs one worker: receives fd+payload from dispatcher, creates sessions. Does not return until event_base stops.
 * backend_names: backends this worker serves. Paths for loading config (worker re-loads backends/routing). */
void run_worker(
    std::size_t worker_id,
    int worker_socket_fd,
    const std::vector<std::string>& backend_names,
    const std::string& resolve_base_path,
    const std::string& backends_path,
    const std::string& routing_path);

}  // namespace server
}  // namespace pgpooler
