#include "metric.h"

#include <butil/time.h>
#include <gflags/gflags.h>

#include "index.h"

DEFINE_int32(region_metric_period_s, 3, "region metric period time");
static bvar::GFlag gflag_region_metric_period_s("region_metric_period_s");
DEFINE_bool(enable_region_metric_report, true,
            "enable region metric report to txplanner");
static bvar::GFlag gflag_enable_region_metric_report(
    "enable_region_metric_report");

namespace azino {
namespace txindex {
RegionMetric::RegionMetric(KVRegion *region, brpc::Channel *txplaner_channel)
    : write("azino_txindex_region_" + region->Describe(), "write_us",
            FLAGS_region_metric_period_s),
      write_error("azino_txindex_region_" + region->Describe(),
                  "write_error_us", FLAGS_region_metric_period_s),
      write_success("azino_txindex_region_" + region->Describe(),
                    "write_success_us", FLAGS_region_metric_period_s),
      read("azino_txindex_region_" + region->Describe(), "read_us",
           FLAGS_region_metric_period_s),
      read_error("azino_txindex_region_" + region->Describe(), "read_error_us",
                 FLAGS_region_metric_period_s),
      read_success("azino_txindex_region_" + region->Describe(),
                   "read_success_us", FLAGS_region_metric_period_s),
      _region(region),
      _txplanner_stub(txplaner_channel) {
    fn = RegionMetric::execute;
}

void RegionMetric::RecordRead(const TxOpStatus &read_status,
                              int64_t start_time) {
    auto latency = butil::gettimeofday_us() - start_time;
    read << latency;
    switch (read_status.error_code()) {
        case TxOpStatus_Code_Ok:
            read_success << latency;
            break;
        default:
            read_error << latency;
    }
}

void RegionMetric::RecordWrite(const TxOpStatus &write_status,
                               int64_t start_time) {
    auto latency = butil::gettimeofday_us() - start_time;
    write << latency;
    switch (write_status.error_code()) {
        case TxOpStatus_Code_Ok:
            write_success << latency;
            break;
        default:
            write_error << latency;
    }
}

void RegionMetric::report_metric() {
    brpc::Controller cntl;
    azino::txplanner::RegionMetricRequest req;
    azino::txplanner::RegionMetricResponse resp;
    {
        auto range = req.mutable_range();
        *range = _region->GetRange().ToPB();
        auto metric = req.mutable_metric();
        metric->set_read_qps(read.qps());
        metric->set_write_qps(write.qps());
        {
            std::lock_guard<bthread::Mutex> lck(m);
            for (const auto &key : pk) {
                metric->add_pessimism_key(key);
            }
            pk.clear();
        }
    }

    _txplanner_stub.RegionMetric(&cntl, &req, &resp, NULL);

    if (cntl.Failed()) {
        LOG(WARNING) << "Controller failed error code: " << cntl.ErrorCode()
                     << " error text: " << cntl.ErrorText();
    }
}

void *RegionMetric::execute(void *args) {
    auto p = reinterpret_cast<RegionMetric *>(args);
    while (true) {
        bthread_usleep(FLAGS_region_metric_period_s * 1000 * 1000);
        {
            std::lock_guard<bthread::Mutex> lck(p->_mutex);
            if (p->_stopped) {
                break;
            }
        }
        p->report_metric();
    }
    return nullptr;
}

void RegionMetric::RecordPessimismKey(const std::string &key) {
    std::lock_guard<bthread::Mutex> lck(m);
    pk.insert(key);
}

}  // namespace txindex
}  // namespace azino