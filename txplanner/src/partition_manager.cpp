#include "partition_manager.h"

namespace azino {
namespace txplanner {
PartitionManager::PartitionManager(azino::Partition initial_partition)
    : partition(initial_partition) {}

void PartitionManager::UpdatePartitionConfigMap(const PartitionConfigMap &pcm) {
    apply_partition_config_map(pcm);
}

azino::Partition PartitionManager::GetPartition() {
    std::lock_guard<bthread::Mutex> lck(m);
    return partition;
}

void PartitionManager::apply_partition_config_map(
    const PartitionConfigMap &pcm) {
    std::lock_guard<bthread::Mutex> lck(m);
    partition.SetPartitionConfigMap(pcm);
}

}  // namespace txplanner
}  // namespace azino