#ifndef AZINO_TXPLANNER_INCLUDE_PARTITION_MANAGER_H
#define AZINO_TXPLANNER_INCLUDE_PARTITION_MANAGER_H

#include <bthread/mutex.h>

#include <map>
#include <string>
#include <utility>

#include "azino/partition.h"

namespace azino {
namespace txplanner {

class PartitionManager {
   public:
    PartitionManager(azino::Partition initial_partition);
    azino::Partition GetPartition();
    void UpdatePartitionConfigMap(const RangeSet& to_del_ranges,
                                  const PartitionConfigMap& to_add_ranges);

   private:
    void apply_partition_config_map(const RangeSet& to_del_ranges,
                                    const PartitionConfigMap& to_add_ranges);

    bthread::Mutex m;
    azino::Partition partition;
};
}  // namespace txplanner
}  // namespace azino

#endif  // AZINO_TXPLANNER_INCLUDE_PARTITION_MANAGER_H
