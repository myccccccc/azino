#include <brpc/server.h>
#include <butil/fast_rand.h>
#include <butil/logging.h>
#include <gflags/gflags.h>

DEFINE_string(listen_addr, "0.0.0.0:8001", "Listen addr");
DEFINE_string(storage_addr, "0.0.0.0:8000", "Address of storage");
DEFINE_string(txindex_addrs, "0.0.0.0:8002",
              "Addresses of txindexes, split by space");
DEFINE_int64(partition_num_per_txindex, 64, "partitions number in one txindex");
DEFINE_int64(record_count, 100000, "total record count");
DEFINE_string(log_file, "log_txplanner", "log file name for txplanner");

namespace logging {
DECLARE_bool(crash_on_fatal_log);
}

#include "azino/comparator.h"
#include "planner.h"
#include "service.h"
#include "txidtable.h"

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
    azino::txplanner::TxIDTable tt;

    std::string txindex_addr;
    std::vector<std::string> txindex_addrs;
    std::stringstream ss(FLAGS_txindex_addrs);
    while (ss >> txindex_addr) {
        txindex_addrs.push_back(txindex_addr);
        txindex_addr.clear();
    }

    std::vector<std::string> records;
    for (int i = 0; i < FLAGS_record_count; i++) {
        records.push_back(std::to_string(i));
    }

    azino::BitWiseComparator cmp;
    std::sort(records.begin(), records.end(), cmp);

    int partition_record_count =
        records.size() /
        (txindex_addrs.size() * FLAGS_partition_num_per_txindex);
    std::deque<std::string> partition_endpoints;
    for (int i = partition_record_count; i < records.size();
         i += partition_record_count) {
        partition_endpoints.push_back(records[i]);
    }

    if (partition_endpoints.size() >
        (txindex_addrs.size() * FLAGS_partition_num_per_txindex) - 1) {
        partition_endpoints.pop_back();
    }

    partition_endpoints.emplace_front("");
    partition_endpoints.emplace_back("");

    azino::PartitionConfigMap pcm;
    size_t index = 0;
    for (auto& txindex_addr : txindex_addrs) {
        for (auto i = 0; i < FLAGS_partition_num_per_txindex &&
                         index < partition_endpoints.size() - 1;
             i++, index++) {
            auto range = azino::Range(partition_endpoints[index],
                                      partition_endpoints[index + 1], 1, 0);
            pcm.insert(
                std::make_pair(range, azino::PartitionConfig(txindex_addr)));
            LOG(WARNING) << "Txindex:" << txindex_addr << " Range" << i << ":"
                         << range.Describe();
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
    if (server.Start(FLAGS_listen_addr.c_str(), &options) != 0) {
        LOG(FATAL) << "Fail to start TxPlannerServer";
        return -1;
    }

    server.RunUntilAskedToQuit();
    return 0;
}
