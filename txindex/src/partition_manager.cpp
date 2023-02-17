#include "partition_manager.h"

namespace azino {
namespace txindex {
PartitionManager::PartitionManager(brpc::Channel *txplaner_channel)
    : _stub(txplaner_channel) {
    update_partition();
}

void PartitionManager::update_partition() {
    brpc::Controller cntl;
    azino::txplanner::GetPartitionRequest req;
    azino::txplanner::GetPartitionResponse resp;

    _stub.GetPartition(&cntl, &req, &resp, NULL);
    if (cntl.Failed()) {
        LOG(WARNING) << "Controller failed error code: " << cntl.ErrorCode()
                     << " error text: " << cntl.ErrorText();
    }

    p = Partition::FromPB(resp.partition());
}
}  // namespace txindex
}  // namespace azino