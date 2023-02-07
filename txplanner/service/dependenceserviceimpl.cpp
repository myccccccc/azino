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

    auto p1_p2 = _tt->AddDep(DepType::READWRITE, request->t1(), request->t2());
    if (p1_p2.first == nullptr || p1_p2.second == nullptr) {
        return;
    }

    LOG(INFO) << cntl->remote_side() << " Dep report type:"
              << "readwrite"
              << " key:" << request->key()
              << " t1:" << request->t1().ShortDebugString()
              << " t2:" << request->t2().ShortDebugString();

    done_guard.release()->Run();

    auto abort_set = _tt->FindAbortTxnOnConsecutiveRWDep(p1_p2.first);
    for (auto p : abort_set) {
        LOG(INFO) << " tx: " << p->txid.ShortDebugString() << " will be abort.";
        _tt->AbortTx(p->txid);
    }
    abort_set = _tt->FindAbortTxnOnConsecutiveRWDep(p1_p2.second);
    for (auto p : abort_set) {
        LOG(INFO) << " tx: " << p->txid.ShortDebugString() << " will be abort.";
        _tt->AbortTx(p->txid);
    }
}

}  // namespace txplanner
}  // namespace azino