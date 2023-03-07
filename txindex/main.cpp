#include <brpc/channel.h>
#include <brpc/server.h>
#include <butil/logging.h>
#include <gflags/gflags.h>

#include "index.h"
#include "service.h"

DEFINE_string(listen_addr, "0.0.0.0:8002", "Addresses of txindex");
DEFINE_string(txindex_addr, "0.0.0.0:8002", "Addresses of txindex");
DEFINE_string(txplanner_addr, "0.0.0.0:8001", "Address of txplanner");
DEFINE_string(log_file, "log_txindex", "log file name for txindex");

namespace logging {
DECLARE_bool(crash_on_fatal_log);
}

namespace bthread {
DECLARE_int32(bthread_concurrency);
}

int main(int argc, char* argv[]) {
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    logging::FLAGS_crash_on_fatal_log = true;
    logging::LoggingSettings log_settings;
    log_settings.logging_dest = logging::LoggingDestination::LOG_TO_FILE;
    auto log_file = FLAGS_log_file + FLAGS_txindex_addr;
    log_settings.log_file = log_file.c_str();
    log_settings.delete_old =
        logging::OldFileDeletionState::DELETE_OLD_LOG_FILE;
    logging::InitLogging(log_settings);

    brpc::Server server;
    brpc::ServerOptions server_options;
    server_options.num_threads = bthread::FLAGS_bthread_concurrency;
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

    if (server.Start(FLAGS_listen_addr.c_str(), &server_options) != 0) {
        LOG(FATAL) << "Fail to start TxIndexServer";
        return -1;
    }

    server.RunUntilAskedToQuit();

    delete txindex;
}