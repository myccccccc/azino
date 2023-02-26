#ifndef AZINO_TXINDEX_INCLUDE_METRIC_H
#define AZINO_TXINDEX_INCLUDE_METRIC_H

#include <brpc/channel.h>
#include <butil/macros.h>
#include <butil/time.h>
#include <bvar/bvar.h>
#include <gflags/gflags.h>

#include <memory>
#include <unordered_set>

#include "azino/background_task.h"
#include "bthread/bthread.h"
#include "bthread/mutex.h"
#include "mvccvalue.h"
#include "service/storage/storage.pb.h"
#include "service/txplanner/txplanner.pb.h"

DECLARE_bool(enable_region_metric_report);

namespace azino {
namespace txindex {
class KVRegion;
class RegionMetric : public azino::BackgroundTask {
   public:
    RegionMetric(KVRegion* region, brpc::Channel* txplaner_channel);
    DISALLOW_COPY_AND_ASSIGN(RegionMetric);
    ~RegionMetric() = default;

    void RecordRead(const TxOpStatus& read_status, int64_t start_time);
    void RecordWrite(const TxOpStatus& write_status, int64_t start_time);
    void RecordPessimismKey(const std::string& key);

   private:
    void report_metric();
    static void* execute(void* args);

    // write
    bvar::LatencyRecorder write;  // total write latency(us)
    bvar::LatencyRecorder
        write_error;  // write error(conflict, too late, block) latency(us)
    bvar::LatencyRecorder write_success;  // write success latency(us)

    // read
    bvar::LatencyRecorder read;  // total read latency(us)
    bvar::LatencyRecorder
        read_error;  // read error(not exist, block) latency(us)
    bvar::LatencyRecorder read_success;  // read success latency(us)

    bthread::Mutex m;
    std::unordered_set<std::string> pk;  // pessimism key

    KVRegion* _region;
    txplanner::RegionService_Stub _txplanner_stub;
};
}  // namespace txindex
}  // namespace azino

#endif  // AZINO_TXINDEX_INCLUDE_METRIC_H
