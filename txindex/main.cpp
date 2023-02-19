#include <brpc/channel.h>
#include <brpc/server.h>
#include <butil/logging.h>
#include <gflags/gflags.h>

#include "index.h"
#include "service.h"

DEFINE_string(txindex_addr, "0.0.0.0:8002", "Addresses of txindex");
DEFINE_string(txplanner_addr, "0.0.0.0:8001", "Address of txplanner");

namespace logging {
DECLARE_bool(crash_on_fatal_log);
}

int main(int argc, char* argv[]) {
    logging::FLAGS_crash_on_fatal_log = true;
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    brpc::Server server;
    brpc::ServerOptions server_options;
    brpc::Channel txplanner_channel;
    brpc::ChannelOptions options;

    if (txplanner_channel.Init(FLAGS_txplanner_addr.c_str(), &options) != 0) {
        LOG(FATAL) << "Fail to initialize txplanner channel";
    }

    auto txindex = new azino::txindex::TxIndex(&txplanner_channel);

    azino::txindex::TxOpServiceImpl tx_op_service_impl(txindex);
    if (server.AddService(&tx_op_service_impl,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add tx_op_service_impl";
        return -1;
    }

    if (server.Start(FLAGS_txindex_addr.c_str(), &server_options) != 0) {
        LOG(FATAL) << "Fail to start TxIndexServer";
        return -1;
    }

    server.RunUntilAskedToQuit();

    delete txindex;
}