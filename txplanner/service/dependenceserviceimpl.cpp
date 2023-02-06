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

    auto tx1_ts = request->tx1_ts();
    auto tx2_ts = request->tx2_ts();

    done_guard.release()->Run();

    auto abort_set = _tt->FindAbortTxnOnConsecutiveRWDep(tx1_ts);
    for (auto p : abort_set) {
        LOG(INFO) << " tx: " << p->txid.ShortDebugString() << " will be abort.";
        _tt->AbortTx(p->txid);
    }
    abort_set = _tt->FindAbortTxnOnConsecutiveRWDep(tx2_ts);
    for (auto p : abort_set) {
        LOG(INFO) << " tx: " << p->txid.ShortDebugString() << " will be abort.";
        _tt->AbortTx(p->txid);
    }
}

}  // namespace txplanner
}  // namespace azino