#include <brpc/server.h>
#include <butil/logging.h>
#include <gflags/gflags.h>

DEFINE_string(listen_addr, "0.0.0.0:8000", "Listen addr");
DEFINE_string(log_file, "log_storage", "log file name for storage");
namespace logging {
DECLARE_bool(crash_on_fatal_log);
}

#include "service.h"

namespace azino {
namespace storage {}  // namespace storage
}  // namespace azino

int main(int argc, char* argv[]) {
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    logging::FLAGS_crash_on_fatal_log = true;
    logging::LoggingSettings log_settings;
    log_settings.logging_dest = logging::LoggingDestination::LOG_TO_FILE;
    log_settings.log_file = FLAGS_log_file.c_str();
    log_settings.delete_old =
        logging::OldFileDeletionState::DELETE_OLD_LOG_FILE;
    logging::InitLogging(log_settings);

    brpc::Server server;

    azino::storage::StorageServiceImpl storage_service_impl;
    if (server.AddService(&storage_service_impl,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add storage_service_impl";
        return -1;
    }

    brpc::ServerOptions options;
    if (server.Start(FLAGS_listen_addr.c_str(), &options) != 0) {
        LOG(FATAL) << "Fail to start StorageServer";
        return -1;
    }

    server.RunUntilAskedToQuit();
    return 0;
}
