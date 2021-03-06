#include <gflags/gflags.h>
#include <butil/logging.h>
#include <brpc/server.h>
#include <toml/toml.hpp>

DEFINE_string(storage_addr, "0.0.0.0:8000", "Address of storage");
DEFINE_string(txplanner_addr, "0.0.0.0:8001", "Address of txplanner");
DEFINE_string(txindex_addrs, "0.0.0.0:8002", "Addresses of txindexes, split by space");
namespace logging {
    DECLARE_bool(crash_on_fatal_log);
}

#include "service.h"

namespace azino {
namespace storage {

} // namespace storage
} // namespace azino

int main(int argc, char* argv[]) {
    logging::FLAGS_crash_on_fatal_log = true;
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    brpc::Server server;

    std::string txindex_addr;
    std::vector<std::string> txindex_addrs;
    std::istringstream iss(FLAGS_txindex_addrs);
    while (iss >> txindex_addr) {
        txindex_addrs.push_back(txindex_addr);
    }

    azino::txplanner::TxServiceImpl tx_service_impl(txindex_addrs, FLAGS_storage_addr);
    if (server.AddService(&tx_service_impl,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add tx_service_impl";
        return -1;
    }

    brpc::ServerOptions options;
    if (server.Start(FLAGS_txplanner_addr.c_str(), &options) != 0) {
        LOG(FATAL) << "Fail to start TxPlannerServer";
        return -1;
    }

    server.RunUntilAskedToQuit();
    return 0;
}

