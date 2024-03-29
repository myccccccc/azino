#include "metric.h"

#include <butil/time.h>

namespace azino {
namespace txplanner {

TxMetric::TxMetric()
    : commit("azino_txplanner", "commit_ms", 1),
      abort("azino_txplanner", "abort_ms", 1) {
    fn = TxMetric::execute;
}

void TxMetric::RecordCommit(const TxIDPtr &t) {
    auto latency = butil::gettimeofday_ms() - t->begin_time();
    commit << latency;
}

void TxMetric::RecordAbort(const TxIDPtr &t) {
    auto latency = butil::gettimeofday_ms() - t->begin_time();
    abort << latency;
}

void TxMetric::dump() {
    LOG(WARNING) << "commit:" << commit
                 << " P50:" << commit.latency_percentile(0.5)
                 << " P90:" << commit.latency_percentile(0.9)
                 << " P99:" << commit.latency_percentile(0.99);
    LOG(WARNING) << "abort:" << abort
                 << " P50:" << abort.latency_percentile(0.5)
                 << " P90:" << abort.latency_percentile(0.9)
                 << " P99:" << abort.latency_percentile(0.99);
}

void *TxMetric::execute(void *args) {
    auto p = reinterpret_cast<TxMetric *>(args);
    while (true) {
        bthread_usleep(1 * 1000 * 1000);
        {
            std::lock_guard<bthread::Mutex> lck(p->_mutex);
            if (p->_stopped) {
                break;
            }
        }
        p->dump();
    }
    return nullptr;
}

}  // namespace txplanner
}  // namespace azino