#ifndef AZINO_TXPLANNER_INCLUDE_METRIC_H
#define AZINO_TXPLANNER_INCLUDE_METRIC_H

#include <butil/macros.h>
#include <butil/time.h>
#include <bvar/bvar.h>

#include "txid.h"

namespace azino {
namespace txplanner {
class TxMetric {
   public:
    TxMetric();
    DISALLOW_COPY_AND_ASSIGN(TxMetric);
    ~TxMetric() = default;

    void RecordCommit(const TxIDPtr& t);
    void RecordAbort(const TxIDPtr& t);

   private:
    bvar::LatencyRecorder commit;  // total commit latency(ms)
    bvar::LatencyRecorder abort;   // total abort latency(ms)
};
}  // namespace txplanner
}  // namespace azino
#endif  // AZINO_TXPLANNER_INCLUDE_METRIC_H
