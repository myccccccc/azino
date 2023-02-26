#ifndef AZINO_TXPLANNER_INCLUDE_PLANNER_H
#define AZINO_TXPLANNER_INCLUDE_PLANNER_H

#include <bthread/execution_queue.h>

#include "azino/partition.h"
#include "partition_manager.h"
#include "service/txplanner/txplanner.pb.h"

namespace azino {
namespace txplanner {
class CCPlanner {
   public:
    CCPlanner(PartitionManager* pm);
    ~CCPlanner();
    void ReportMetric(const Range& range, const RegionMetric& metric);

   private:
    static int execute(
        void* args,
        bthread::TaskIterator<std::pair<Range, RegionMetric>>& iter);

    void plan(const Range& range, const RegionMetric& metric,
              RangeSet& to_del_ranges, PartitionConfigMap& to_add_ranges);

    PartitionManager* _pm;
    PartitionConfigMap _m;
    bthread::ExecutionQueueId<std::pair<Range, RegionMetric>> _queue;
};
}  // namespace txplanner
}  // namespace azino

#endif  // AZINO_TXPLANNER_INCLUDE_PLANNER_H
