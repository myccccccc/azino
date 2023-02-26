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
        LOG(ERROR) << "fail to stop execution queue in CCPlanner";
    }
    if (bthread::execution_queue_join(_queue) != 0) {
        LOG(ERROR) << "fail to join execution queue in CCPlanner";
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
        RangeSet rs;
        PartitionConfigMap pcm;
        p->plan(range, rm, rs, pcm);
        p->_pm->UpdatePartitionConfigMap(rs, pcm);
    }

    return 0;
}

void CCPlanner::plan(const Range &range, const RegionMetric &metric,
                     RangeSet &to_del_ranges,
                     PartitionConfigMap &to_add_ranges) {
    // TODO: split or merge
    auto iter = _m.find(range);
    if (iter == _m.end()) {
        LOG(ERROR) << "CCPlanner fail to find metric range:"
                   << range.Describe();
        return;
    }
    auto new_config = iter->second;
    _m.erase(iter);
    to_del_ranges.insert(range);

    auto &pk = new_config.MutablePessimismKey();
    pk.clear();
    for (int i = 0; i < metric.pessimism_key_size(); i++) {
        pk.insert(metric.pessimism_key(i));
    }
    _m.insert(std::make_pair(range, new_config));
    to_add_ranges.insert(std::make_pair(range, new_config));

    return;
}

}  // namespace txplanner
}  // namespace azino