#include "metric.h"

#include <butil/time.h>

namespace azino {
namespace txplanner {

TxMetric::TxMetric()
    : commit("azino_txplanner", "commit_ms", 1),
      abort("azino_txplanner", "abort_ms", 1) {}

void TxMetric::RecordCommit(const TxIDPtr &t) {
    auto latency = butil::gettimeofday_ms() - t->begin_time();
    commit << latency;
}

void TxMetric::RecordAbort(const TxIDPtr &t) {
    auto latency = butil::gettimeofday_ms() - t->begin_time();
    abort << latency;
}
}  // namespace txplanner
}  // namespace azino