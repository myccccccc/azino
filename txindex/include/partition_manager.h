#ifndef AZINO_TXINDEX_INCLUDE_PARTITION_MANAGER_H
#define AZINO_TXINDEX_INCLUDE_PARTITION_MANAGER_H

#include <brpc/channel.h>

#include "azino/partition.h"
#include "service/txplanner/txplanner.pb.h"

namespace azino {
namespace txindex {
class PartitionManager {
   public:
    PartitionManager(brpc::Channel* txplaner_channel);
    ~PartitionManager() = default;
    inline Partition GetPartition() { return p; }

   private:
    void update_partition();
    Partition p;
    txplanner::PartitionService_Stub _stub;
};
}  // namespace txindex
}  // namespace azino

#endif  // AZINO_TXINDEX_INCLUDE_PARTITION_MANAGER_H
