#include <brpc/server.h>
#include <butil/logging.h>
#include <gflags/gflags.h>

DEFINE_string(server, "0.0.0.0:8000", "Address of server");
namespace logging {
DECLARE_bool(crash_on_fatal_log);
}

#include "service.h"

namespace azino {
namespace storage {}  // namespace storage
}  // namespace azino

int main(int argc, char* argv[]) {
    logging::FLAGS_crash_on_fatal_log = true;
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    brpc::Server server;

    azino::storage::StorageServiceImpl storage_service_impl;
    if (server.AddService(&storage_service_impl,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add storage_service_impl";
        return -1;
    }

    brpc::ServerOptions options;
    if (server.Start(FLAGS_server.c_str(), &options) != 0) {
        LOG(FATAL) << "Fail to start StorageServer";
        return -1;
    }

    server.RunUntilAskedToQuit();
    return 0;
}
