#include <brpc/server.h>
#include <bthread/mutex.h>

#include "azino/kv.h"
#include "service.h"

namespace azino {
namespace txplanner {
class AscendingTimer {
   public:
    AscendingTimer(TimeStamp t) : _ts(t) {}
    DISALLOW_COPY_AND_ASSIGN(AscendingTimer);
    ~AscendingTimer() = default;
    TimeStamp NewTime() {
        std::lock_guard<bthread::Mutex> lck(_m);
        auto tmp = ++_ts;
        return tmp;
    }

   private:
    TimeStamp _ts;
    bthread::Mutex _m;
};

TxServiceImpl::TxServiceImpl(const std::vector<std::string> &txindex_addrs,
                             const std::string &storage_adr, TxIDTable *tt)
    : _timer(new AscendingTimer(MIN_TIMESTAMP)),
      _tt(tt),
      _txindex_addrs(txindex_addrs),
      _storage_addr(storage_adr) {}

TxServiceImpl::~TxServiceImpl() {}

void TxServiceImpl::BeginTx(::google::protobuf::RpcController *controller,
                            const ::azino::txplanner::BeginTxRequest *request,
                            ::azino::txplanner::BeginTxResponse *response,
                            ::google::protobuf::Closure *done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

    auto start_ts = _timer->NewTime();
    auto txidptr = _tt->BeginTx(start_ts);
    auto txid = new TxIdentifier(txidptr->get_txid());
    response->set_allocated_txid(txid);

    for (std::string &addr : _txindex_addrs) {
        response->add_txindex_addrs(addr);
    }
    response->set_storage_addr(_storage_addr);

    LOG(INFO) << cntl->remote_side() << " tx: " << txid->ShortDebugString()
              << " is going to begin.";
}

void TxServiceImpl::CommitTx(::google::protobuf::RpcController *controller,
                             const ::azino::txplanner::CommitTxRequest *request,
                             ::azino::txplanner::CommitTxResponse *response,
                             ::google::protobuf::Closure *done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

    if (request->txid().status().status_code() != TxStatus_Code_Preput) {
        LOG(WARNING) << cntl->remote_side()
                     << " tx: " << request->txid().ShortDebugString()
                     << " are not supposed to commit.";
    }

    auto commit_ts = _timer->NewTime();
    auto txidptr = _tt->CommitTx(request->txid(), commit_ts);
    auto txid = new TxIdentifier(txidptr->get_txid());
    response->set_allocated_txid(txid);

    LOG(INFO) << cntl->remote_side() << " tx: " << txid->ShortDebugString()
              << " is going to commit.";
    txidptr->set_finished_by_client();
    done_guard.release()->Run();

    if (txidptr->get_txid().status().status_code() == TxStatus_Code_Commit) {
        auto abort_set = _tt->FindAbortTxnOnConsecutiveRWDep(txidptr);
        for (auto p : abort_set) {
            LOG(INFO) << " tx: " << p->get_txid().ShortDebugString()
                      << " will be abort.";
            _tt->AbortTx(p->get_txid());
        }
    }
}

void TxServiceImpl::AbortTx(::google::protobuf::RpcController *controller,
                            const ::azino::txplanner::AbortTxRequest *request,
                            ::azino::txplanner::AbortTxResponse *response,
                            ::google::protobuf::Closure *done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

    if (request->txid().status().status_code() != TxStatus_Code_Preput) {
        LOG(WARNING) << cntl->remote_side()
                     << " tx: " << request->txid().ShortDebugString()
                     << " are not supposed to abort.";
    }

    auto txidptr = _tt->AbortTx(request->txid());
    auto txid = new TxIdentifier(txidptr->get_txid());
    response->set_allocated_txid(txid);

    LOG(INFO) << cntl->remote_side() << " tx: " << txid->ShortDebugString()
              << " is going to abort.";
    txidptr->set_finished_by_client();
}

void TxServiceImpl::ValidateTx(
    ::google::protobuf::RpcController *controller,
    const ::azino::txplanner::ValidateTxRequest *request,
    ::azino::txplanner::ValidateTxResponse *response,
    ::google::protobuf::Closure *done) {
    brpc::ClosureGuard done_guard(done);
    if (request->is_early_validation()) {
        _tt->EarlyValidateTxID(request->txid(), response, done_guard.release());
        return;
    }
}
}  // namespace txplanner
}  // namespace azino