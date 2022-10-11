#include <brpc/server.h>
#include <bthread/mutex.h>

#include "azino/kv.h"
#include "service.h"

namespace azino {
namespace txplanner {

DependenceServiceImpl::DependenceServiceImpl() {}

DependenceServiceImpl::~DependenceServiceImpl() {}

void DependenceServiceImpl::RWDep(::google::protobuf::RpcController* controller,
                                  const ::azino::txplanner::DepRequest* request,
                                  ::azino::txplanner::DepResponse* response,
                                  ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << cntl->remote_side() << " Dep report type:"
              << "readwrite"
              << " key:" << request->key() << " ts1:" << request->tx1_ts()
              << " ts2:" << request->tx2_ts();
}
void DependenceServiceImpl::WWDep(::google::protobuf::RpcController* controller,
                                  const ::azino::txplanner::DepRequest* request,
                                  ::azino::txplanner::DepResponse* response,
                                  ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << cntl->remote_side() << " Dep report type:"
              << "writewrite"
              << " key:" << request->key() << " ts1:" << request->tx1_ts()
              << " ts2:" << request->tx2_ts();
}
void DependenceServiceImpl::WRDep(::google::protobuf::RpcController* controller,
                                  const ::azino::txplanner::DepRequest* request,
                                  ::azino::txplanner::DepResponse* response,
                                  ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << cntl->remote_side() << " Dep report type:"
              << "writeread"
              << " key:" << request->key() << " ts1:" << request->tx1_ts()
              << " ts2:" << request->tx2_ts();
}

}  // namespace txplanner
}  // namespace azino