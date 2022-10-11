#include <brpc/server.h>

#include "index.h"
#include "reporter.h"
#include "service.h"

#define DO_DEP_REPORT(key, deps)                                              \
    do {                                                                      \
        for (size_t i = 0; i < deps.size(); i++) {                            \
            switch (deps[i].type) {                                           \
                case DepType::READWRITE:                                      \
                    _deprpt->ReadWriteReport(key, deps[i].ts1, deps[i].ts2);  \
                    break;                                                    \
                case DepType::WRITEREAD:                                      \
                    _deprpt->WriteReadReport(key, deps[i].ts1, deps[i].ts2);  \
                    break;                                                    \
                case DepType::WRITEWRITE:                                     \
                    _deprpt->WriteWriteReport(key, deps[i].ts1, deps[i].ts2); \
                    break;                                                    \
            }                                                                 \
        }                                                                     \
    } while (0);

namespace azino {
namespace txindex {
TxOpServiceImpl::TxOpServiceImpl(const std::string& storage_addr,
                                 const std::string& txplanner_addr)
    : _index(TxIndex::DefaultTxIndex(storage_addr)),
      _deprpt(new DepReporter(txplanner_addr)) {}
TxOpServiceImpl::~TxOpServiceImpl() = default;

void TxOpServiceImpl::WriteIntent(
    ::google::protobuf::RpcController* controller,
    const ::azino::txindex::WriteIntentRequest* request,
    ::azino::txindex::WriteIntentResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    std::vector<Dep> deps;
    TxOpStatus* sts = new TxOpStatus(_index->WriteIntent(
        request->key(), request->value(), request->txid(), deps));

    DO_DEP_REPORT(request->key(), deps)

    LOG(INFO) << cntl->remote_side()
              << " tx: " << request->txid().ShortDebugString()
              << " write intent"
              << " key: " << request->key()
              << " value: " << request->value().ShortDebugString()
              << " error code: " << sts->error_code()
              << " error message: " << sts->error_message();

    response->set_allocated_tx_op_status(sts);
}

void TxOpServiceImpl::WriteLock(
    ::google::protobuf::RpcController* controller,
    const ::azino::txindex::WriteLockRequest* request,
    ::azino::txindex::WriteLockResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    std::vector<Dep> deps;
    TxOpStatus* sts = new TxOpStatus(
        _index->WriteLock(request->key(), request->txid(),
                          std::bind(&TxOpServiceImpl::WriteLock, this,
                                    controller, request, response, done),
                          deps));

    DO_DEP_REPORT(request->key(), deps)

    LOG(INFO) << cntl->remote_side()
              << " tx: " << request->txid().ShortDebugString() << " write lock"
              << " key: " << request->key()
              << " error code: " << sts->error_code()
              << " error message: " << sts->error_message();

    if (sts->error_code() == TxOpStatus_Code_WriteBlock) {
        done_guard.release();
        delete sts;
    } else {
        response->set_allocated_tx_op_status(sts);
    }
}

void TxOpServiceImpl::Clean(::google::protobuf::RpcController* controller,
                            const ::azino::txindex::CleanRequest* request,
                            ::azino::txindex::CleanResponse* response,
                            ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    TxOpStatus* sts =
        new TxOpStatus(_index->Clean(request->key(), request->txid()));

    LOG(INFO) << cntl->remote_side()
              << " tx: " << request->txid().ShortDebugString() << " clean"
              << " key: " << request->key()
              << " error code: " << sts->error_code()
              << " error message: " << sts->error_message();

    response->set_allocated_tx_op_status(sts);
}

void TxOpServiceImpl::Commit(::google::protobuf::RpcController* controller,
                             const ::azino::txindex::CommitRequest* request,
                             ::azino::txindex::CommitResponse* response,
                             ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    TxOpStatus* sts =
        new TxOpStatus(_index->Commit(request->key(), request->txid()));

    LOG(INFO) << cntl->remote_side()
              << " tx: " << request->txid().ShortDebugString() << " commit"
              << " key: " << request->key()
              << " error code: " << sts->error_code()
              << " error message: " << sts->error_message();

    response->set_allocated_tx_op_status(sts);
}

void TxOpServiceImpl::Read(::google::protobuf::RpcController* controller,
                           const ::azino::txindex::ReadRequest* request,
                           ::azino::txindex::ReadResponse* response,
                           ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    Value* v = new Value();
    std::vector<Dep> deps;
    TxOpStatus* sts = new TxOpStatus(
        _index->Read(request->key(), *v, request->txid(),
                     std::bind(&TxOpServiceImpl::Read, this, controller,
                               request, response, done),
                     deps));

    DO_DEP_REPORT(request->key(), deps)

    LOG(INFO) << cntl->remote_side()
              << " tx: " << request->txid().ShortDebugString() << " read"
              << " key: " << request->key()
              << " error code: " << sts->error_code()
              << " error message: " << sts->error_message();

    if (sts->error_code() == TxOpStatus_Code_ReadBlock) {
        done_guard.release();
        delete sts;
        delete v;
    } else {
        response->set_allocated_tx_op_status(sts);
        response->set_allocated_value(v);
    }
}
}  // namespace txindex
}  // namespace azino