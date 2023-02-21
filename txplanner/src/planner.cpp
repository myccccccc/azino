#include "planner.h"

namespace azino {
namespace txplanner {

CCPlanner::CCPlanner(PartitionManager *pm)
    : _pm(pm), _m(pm->GetPartition().GetPartitionConfigMap()) {
    bthread::ExecutionQueueOptions options;
    if (bthread::execution_queue_start(&_queue, &options, CCPlanner::execute,
                                       this) != 0) {
        LOG(ERROR) << "fail to start execution queue in CCPlanner";
    }
}

CCPlanner::~CCPlanner() {
    if (bthread::execution_queue_stop(_queue) != 0) {
        LOG(ERROR) << "fail to stop execution queue in Dependence";
    }
    if (bthread::execution_queue_join(_queue) != 0) {
        LOG(ERROR) << "fail to join execution queue in Dependence";
    }
}

void CCPlanner::ReportMetric(const Range &range, const RegionMetric &metric) {
    if (bthread::execution_queue_execute(_queue,
                                         std::make_pair(range, metric)) != 0) {
        LOG(ERROR) << "fail to add task execution queue in CCPlanner";
    }
}

int CCPlanner::execute(
    void *args, bthread::TaskIterator<std::pair<Range, RegionMetric>> &iter) {
    auto p = reinterpret_cast<CCPlanner *>(args);
    if (iter.is_queue_stopped()) {
        return 0;
    }
    for (; iter; ++iter) {
        auto &range = iter->first;
        auto &rm = iter->second;
        p->plan(range, rm);
        p->_pm->UpdatePartitionConfigMap(p->_m);
    }

    return 0;
}

void CCPlanner::plan(const Range &range, const RegionMetric &metric) { return; }

}  // namespace txplanner
}  // namespace azino