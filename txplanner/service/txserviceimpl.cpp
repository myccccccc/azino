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
                             const std::string &storage_adr)
    : _timer(new AscendingTimer(MIN_TIMESTAMP)),
      _txindex_addrs(txindex_addrs),
      _storage_addr(storage_adr) {}

TxServiceImpl::~TxServiceImpl() {}

void TxServiceImpl::BeginTx(::google::protobuf::RpcController *controller,
                            const ::azino::txplanner::BeginTxRequest *request,
                            ::azino::txplanner::BeginTxResponse *response,
                            ::google::protobuf::Closure *done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

    std::stringstream ss;
    auto txstatus = new TxStatus();
    txstatus->set_status_code(TxStatus_Code_Started);
    auto start_ts = _timer->NewTime();
    auto txid = new TxIdentifier();
    txid->set_start_ts(start_ts);
    txid->set_allocated_status(txstatus);
    ss << cntl->remote_side() << " tx: " << txid->ShortDebugString()
       << " is going to begin.";
    LOG(INFO) << ss.str();
    response->set_allocated_txid(txid);
    for (std::string &addr : _txindex_addrs) {
        response->add_txindex_addrs(addr);
    }
    response->set_storage_addr(_storage_addr);
}

void TxServiceImpl::CommitTx(::google::protobuf::RpcController *controller,
                             const ::azino::txplanner::CommitTxRequest *request,
                             ::azino::txplanner::CommitTxResponse *response,
                             ::google::protobuf::Closure *done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

    if (request->txid().status().status_code() != TxStatus_Code_Started) {
        LOG(WARNING) << cntl->remote_side()
                     << " tx: " << request->txid().ShortDebugString()
                     << " are not supposed to commit.";
    }

    std::stringstream ss;
    auto txstatus = new TxStatus();
    txstatus->set_status_code(TxStatus_Code_Preputting);
    auto commit_ts = _timer->NewTime();
    auto txid = new TxIdentifier();
    txid->set_start_ts(request->txid().start_ts());
    txid->set_commit_ts(commit_ts);
    txid->set_allocated_status(txstatus);
    ss << cntl->remote_side() << " tx: " << txid->ShortDebugString()
       << " is going to commit.";
    LOG(INFO) << ss.str();
    response->set_allocated_txid(txid);
}
}  // namespace txplanner
}  // namespace azino