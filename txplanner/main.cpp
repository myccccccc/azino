#include <brpc/server.h>
#include <butil/fast_rand.h>
#include <butil/logging.h>
#include <gflags/gflags.h>

DEFINE_string(storage_addr, "0.0.0.0:8000", "Address of storage");
DEFINE_string(txplanner_addr, "0.0.0.0:8001", "Address of txplanner");
DEFINE_string(txindex_addrs, "0.0.0.0:8002",
              "Addresses of txindexes, split by space");
DEFINE_int32(partition_num_per_txindex, 256,
             "partitions number in one txindex");
DEFINE_int32(kv_len, 16, "key value length in partition endpoint");
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

    std::string txindex_addr;
    std::vector<std::string> txindex_addrs;
    std::stringstream ss(FLAGS_txindex_addrs);
    while (ss >> txindex_addr) {
        txindex_addrs.push_back(txindex_addr);
        txindex_addr.clear();
    }

    std::unordered_set<std::string> partition_endpoint_set;
    while (partition_endpoint_set.size() <
           txindex_addr.size() * FLAGS_partition_num_per_txindex - 2) {
        partition_endpoint_set.insert(butil::fast_rand_printable(FLAGS_kv_len));
    }
    std::deque<std::string> partition_endpoint(partition_endpoint_set.begin(),
                                               partition_endpoint_set.end());
    std::sort(partition_endpoint.begin(), partition_endpoint.end());
    partition_endpoint.emplace_front("");
    partition_endpoint.emplace_back("");

    azino::PartitionConfigMap pcm;
    size_t index = 0;
    for (auto& txindex_addr : txindex_addrs) {
        for (auto i = 0; i < FLAGS_partition_num_per_txindex; i++) {
            auto range = azino::Range(partition_endpoint[index],
                                      partition_endpoint[index + 1], 1, 0);
            pcm.insert(
                std::make_pair(range, azino::PartitionConfig(txindex_addr)));
            LOG(INFO) << "Txindex:" << txindex_addr
                      << " Range:" << range.Describe();
        }
    }

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
