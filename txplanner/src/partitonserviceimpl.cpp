#include <brpc/server.h>

#include "service.h"

namespace azino {
namespace txplanner {
PartitionServiceImpl::PartitionServiceImpl(PartitionManager *pm) : _pm(pm) {}

PartitionServiceImpl::~PartitionServiceImpl() {}

void PartitionServiceImpl::GetPartition(
    ::google::protobuf::RpcController *controller,
    const ::azino::txplanner::GetPartitionRequest *request,
    ::azino::txplanner::GetPartitionResponse *response,
    ::google::protobuf::Closure *done) {
    brpc::ClosureGuard done_guard(done);
    //    brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);
    auto pb = new PartitionPB(_pm->GetPartition().ToPB());
    response->set_allocated_partition(pb);
}
}  // namespace txplanner
}  // namespace azino