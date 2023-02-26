#include <brpc/server.h>
#include <butil/logging.h>
#include <gflags/gflags.h>

DEFINE_string(storage_addr, "0.0.0.0:8000", "Address of storage");
DEFINE_string(txplanner_addr, "0.0.0.0:8001", "Address of txplanner");
DEFINE_string(txindex_addrs, "0.0.0.0:8002",
              "Addresses of txindexes, split by space");
namespace logging {
DECLARE_bool(crash_on_fatal_log);
}

#include "planner.h"
#include "service.h"
#include "txidtable.h"

int main(int argc, char* argv[]) {
    logging::FLAGS_crash_on_fatal_log = true;
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    brpc::Server server;
    azino::txplanner::TxIDTable tt;
    azino::PartitionConfigMap pcm;
    pcm.insert(std::make_pair(azino::Range("", "b", 0, 0),
                              azino::PartitionConfig(FLAGS_txindex_addrs)));
    pcm.insert(std::make_pair(azino::Range("b", "c", 1, 0),
                              azino::PartitionConfig(FLAGS_txindex_addrs)));
    pcm.insert(std::make_pair(azino::Range("c", "c", 1, 1),
                              azino::PartitionConfig(FLAGS_txindex_addrs)));
    pcm.insert(std::make_pair(azino::Range("c", "d", 0, 1),
                              azino::PartitionConfig(FLAGS_txindex_addrs)));
    pcm.insert(std::make_pair(azino::Range("d", "", 0, 0),
                              azino::PartitionConfig(FLAGS_txindex_addrs)));
    azino::txplanner::PartitionManager pm(
        azino::Partition(pcm, FLAGS_storage_addr));
    azino::txplanner::CCPlanner planner(&pm);

    azino::txplanner::TxServiceImpl tx_service_impl(&tt, &pm);
    if (server.AddService(&tx_service_impl, brpc::SERVER_DOESNT_OWN_SERVICE) !=
        0) {
        LOG(FATAL) << "Fail to add tx_service_impl";
        return -1;
    }

    azino::txplanner::RegionServiceImpl region_service_impl(&tt, &planner);
    if (server.AddService(&region_service_impl,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add region_service_impl";
        return -1;
    }

    azino::txplanner::PartitionServiceImpl partition_service_impl(&pm);
    if (server.AddService(&partition_service_impl,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add partition_service_impl";
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
