#include "partition_manager.h"

namespace azino {
namespace txplanner {
PartitionManager::PartitionManager(azino::Partition initial_partition)
    : partition(initial_partition) {}
}  // namespace txplanner
}  // namespace azino