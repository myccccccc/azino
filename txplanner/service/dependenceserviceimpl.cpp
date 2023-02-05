#include <brpc/server.h>
#include <bthread/mutex.h>

#include "azino/kv.h"
#include "service.h"

namespace azino {
namespace txplanner {

DependenceServiceImpl::DependenceServiceImpl(TxIDTable* tt) : _tt(tt) {}

DependenceServiceImpl::~DependenceServiceImpl() {}

void DependenceServiceImpl::RWDep(::google::protobuf::RpcController* controller,
                                  const ::azino::txplanner::DepRequest* request,
                                  ::azino::txplanner::DepResponse* response,
                                  ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    int error_code =
        _tt->AddDep(DepType::READWRITE, request->tx1_ts(), request->tx2_ts());

    LOG(INFO) << cntl->remote_side() << " Dep report type:"
              << "readwrite"
              << " key:" << request->key() << " ts1:" << request->tx1_ts()
              << " ts2:" << request->tx2_ts() << " error_code:" << error_code;
}

}  // namespace txplanner
}  // namespace azino