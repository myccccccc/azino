#include <brpc/server.h>

#include "index.h"
#include "reporter.h"
#include "service.h"

namespace azino {
namespace txindex {
TxOpServiceImpl::TxOpServiceImpl(TxIndex* index) : _index(index) {}
TxOpServiceImpl::~TxOpServiceImpl() = default;

void TxOpServiceImpl::WriteIntent(
    ::google::protobuf::RpcController* controller,
    const ::azino::txindex::WriteIntentRequest* request,
    ::azino::txindex::WriteIntentResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    std::string key = request->key();
    Deps deps;

    TxOpStatus* sts = new TxOpStatus(_index->WriteIntent(
        request->key(), request->value(), request->txid(), deps));

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

    std::string key = request->key();
    Deps deps;

    TxOpStatus* sts = new TxOpStatus(
        _index->WriteLock(request->key(), request->txid(),
                          std::bind(&TxOpServiceImpl::WriteLock, this,
                                    controller, request, response, done),
                          deps));

    LOG(INFO) << cntl->remote_side()
              << " tx: " << request->txid().ShortDebugString() << " write lock"
              << " key: " << request->key()
              << " error code: " << sts->error_code()
              << " error message: " << sts->error_message();

    if (sts->error_code() == TxOpStatus_Code_WriteBlock) {
        done_guard.release();
        delete sts;
        return;
    }
    response->set_allocated_tx_op_status(sts);
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
    std::string key = request->key();
    Deps deps;
    TxOpStatus* sts = new TxOpStatus(
        _index->Read(request->key(), *v, request->txid(),
                     std::bind(&TxOpServiceImpl::Read, this, controller,
                               request, response, done),
                     deps));

    LOG(INFO) << cntl->remote_side()
              << " tx: " << request->txid().ShortDebugString() << " read"
              << " key: " << request->key()
              << " error code: " << sts->error_code()
              << " error message: " << sts->error_message();

    if (sts->error_code() == TxOpStatus_Code_ReadBlock) {
        done_guard.release();
        delete sts;
        delete v;
        return;
    }
    response->set_allocated_tx_op_status(sts);
    response->set_allocated_value(v);
}
}  // namespace txindex
}  // namespace azino