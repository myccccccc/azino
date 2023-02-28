#include "partition_manager.h"

namespace azino {
namespace txplanner {
PartitionManager::PartitionManager(azino::Partition initial_partition)
    : partition(initial_partition) {}

void PartitionManager::UpdatePartitionConfigMap(
    const RangeSet& to_del_ranges, const PartitionConfigMap& to_add_ranges) {
    apply_partition_config_map(to_del_ranges, to_add_ranges);
}

azino::Partition PartitionManager::GetPartition() {
    std::lock_guard<bthread::Mutex> lck(m);
    return partition;
}

void PartitionManager::apply_partition_config_map(
    const RangeSet& to_del_ranges, const PartitionConfigMap& to_add_ranges) {
    std::lock_guard<bthread::Mutex> lck(m);
    auto& pcm = partition.MutablePartitionConfigMap();

    for (auto& range : to_del_ranges) {
        if (pcm.erase(range) == 0) {
            LOG(ERROR) << "PartitionManager fail to erase range:"
                       << range.Describe();
            return;
        }
    }

    for (auto& p : to_add_ranges) {
        auto& range = p.first;
        auto& conf = p.second;
        auto pair = pcm.insert(std::make_pair(range, conf));
        if (!pair.second) {
            LOG(ERROR) << "PartitionManager fail to add range:"
                       << range.Describe() << " already exist range:"
                       << pair.first->first.Describe();
            return;
        }
    }
}

}  // namespace txplanner
}  // namespace azino