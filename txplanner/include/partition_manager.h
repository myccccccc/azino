#ifndef AZINO_TXPLANNER_INCLUDE_PARTITION_MANAGER_H
#define AZINO_TXPLANNER_INCLUDE_PARTITION_MANAGER_H

#include <map>
#include <string>
#include <utility>

#include "azino/partition.h"

namespace azino {
namespace txplanner {

class PartitionManager {
   public:
    PartitionManager(azino::Partition initial_partition);
    inline azino::Partition GetPartition() { return partition; }

   private:
    azino::Partition partition;
};
}  // namespace txplanner
}  // namespace azino

#endif  // AZINO_TXPLANNER_INCLUDE_PARTITION_MANAGER_H
