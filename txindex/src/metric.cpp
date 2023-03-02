#include "metric.h"

#include <butil/time.h>
#include <gflags/gflags.h>

#include "index.h"

DEFINE_int32(region_metric_period_s, 2, "region metric period time");
static bvar::GFlag gflag_region_metric_period_s("region_metric_period_s");
DEFINE_bool(enable_region_metric_report, true,
            "enable region metric report to txplanner");
static bvar::GFlag gflag_enable_region_metric_report(
    "enable_region_metric_report");

DEFINE_double(alpha, 1,
              "alpha hyper parameter when calculating keyPessimismDegree");
static bvar::GFlag gflag_alpha("alpha");
DEFINE_double(lambda, 0.3, "lambda hyper parameter");
static bvar::GFlag gflag_lambda("lambda");

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
        case TxOpStatus_Code_NotExist:
            read_success << latency;
            break;
        default:
            read_error << latency;
    }
}

void RegionMetric::RecordWrite(const std::string &key,
                               const TxOpStatus &write_status,
                               int64_t start_time) {
    std::lock_guard<bthread::Mutex> lck(m);

    auto &key_metric = km[key];
    auto latency = butil::gettimeofday_us() - start_time;
    write << latency;
    key_metric.RecordWrite();

    switch (write_status.error_code()) {
        case TxOpStatus_Code_Ok:
            write_success << latency;
            break;
        default:
            write_error << latency;
            key_metric.RecordWriteError();
    }

    if (key_metric.PessimismDegree() > FLAGS_lambda) {
        pk.insert(key);
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
            if (metric->pessimism_key_size() != 0) {
                LOG(NOTICE)
                    << "Region:" << _region->Describe()
                    << " pessimism_key size:" << metric->pessimism_key_size();
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

void RegionMetric::GCkm(const std::string &key) {
    //    std::lock_guard<bthread::Mutex> lck(m);
    //    km.erase(key);
}

KeyMetric::KeyMetric()
    : _write(),
      _write_window(&_write, FLAGS_region_metric_period_s),
      _write_error(),
      _write_error_window(&_write_error, FLAGS_region_metric_period_s),
      _tx_op_num(),
      _tx_op_num_window(&_tx_op_num, FLAGS_region_metric_period_s),
      _tx_op_after_write_num(),
      _tx_op_after_write_num_window(&_tx_op_after_write_num,
                                    FLAGS_region_metric_period_s) {}

double KeyMetric::PessimismDegree() {
    double write_error_window = (double)_write_error_window.get_value();
    double write_window = (double)_write_window.get_value();
    double c = (int)write_window == 0 ? 0 : write_error_window / write_window;

    double tx_op_after_write_num_window =
        (double)_tx_op_after_write_num_window.get_value();
    double tx_op_num_window = (double)_tx_op_num_window.get_value();
    double l = (int)tx_op_num_window == 0
                   ? 0
                   : tx_op_after_write_num_window / tx_op_num_window;

    double res = FLAGS_alpha * c + (1 - FLAGS_alpha) * l;

    LOG(NOTICE) << "Key Metric:" << write_error_window << " " << write_window
                << " " << c << " " << tx_op_after_write_num_window << " "
                << tx_op_num_window << " " << l << " " << res;
    return res;
}

}  // namespace txindex
}  // namespace azino