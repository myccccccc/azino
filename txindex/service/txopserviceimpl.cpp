#include "service.h"
#include "index.h"

#include <brpc/server.h>

namespace azino {
namespace txindex {
    TxOpServiceImpl::TxOpServiceImpl(const std::string& storage_addr)
    : _index(TxIndex::DefaultTxIndex(storage_addr)) {}
    TxOpServiceImpl::~TxOpServiceImpl() = default;

    void TxOpServiceImpl::WriteIntent(::google::protobuf::RpcController* controller,
                             const ::azino::txindex::WriteIntentRequest* request,
                             ::azino::txindex::WriteIntentResponse* response,
                             ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

        std::stringstream ss;
        ss << cntl->remote_side() << " tx: " << request->txid().ShortDebugString() << " is going to write intent"
           << " key: " << request->key() << " value: " << request->value().ShortDebugString();
        LOG(INFO) << ss.str();

        TxOpStatus* sts = new TxOpStatus(_index->WriteIntent(request->key(), request->value(), request->txid()));
        response->set_allocated_tx_op_status(sts);
    }

    void TxOpServiceImpl::WriteLock(::google::protobuf::RpcController* controller,
                           const ::azino::txindex::WriteLockRequest* request,
                           ::azino::txindex::WriteLockResponse* response,
                           ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

        std::stringstream ss;
        ss << cntl->remote_side() << " tx: " << request->txid().ShortDebugString() << " is going to write lock"
           << " key: " << request->key();
        LOG(INFO) << ss.str();

        TxOpStatus* sts = new TxOpStatus(_index->WriteLock(request->key(), request->txid(),
                                                           std::bind(&TxOpServiceImpl::WriteLockCallBack, this, controller, request, response, done, std::placeholders::_1)));
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
        brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

        std::stringstream ss;
        ss << cntl->remote_side() << " tx: " << request->txid().ShortDebugString() << " is going to clean"
           << " key: " << request->key();
        LOG(INFO) << ss.str();

        TxOpStatus* sts = new TxOpStatus(_index->Clean(request->key(), request->txid()));
        response->set_allocated_tx_op_status(sts);
    }

    void TxOpServiceImpl::Commit(::google::protobuf::RpcController* controller,
                        const ::azino::txindex::CommitRequest* request,
                        ::azino::txindex::CommitResponse* response,
                        ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

        std::stringstream ss;
        ss << cntl->remote_side() << " tx: " << request->txid().ShortDebugString() << " is going to commit"
           << " key: " << request->key();
        LOG(INFO) << ss.str();

        TxOpStatus* sts = new TxOpStatus(_index->Commit(request->key(), request->txid()));
        response->set_allocated_tx_op_status(sts);
    }

    void TxOpServiceImpl::Read(::google::protobuf::RpcController* controller,
                      const ::azino::txindex::ReadRequest* request,
                      ::azino::txindex::ReadResponse* response,
                      ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

        std::stringstream ss;
        ss << cntl->remote_side() << " tx: " << request->txid().ShortDebugString() << " is going to read"
           << " key: " << request->key();
        LOG(INFO) << ss.str();

        Value* v = new Value();
        TxOpStatus* sts = new TxOpStatus(_index->Read(request->key(), *v, request->txid(),
                                                           std::bind(&TxOpServiceImpl::ReadCallBack, this, controller, request, response, done, std::placeholders::_1)));
        if (sts->error_code() == TxOpStatus_Code_ReadBlock) {
            done_guard.release();
            delete sts;
            delete v;
        } else {
            response->set_allocated_tx_op_status(sts);
            response->set_allocated_value(v);
        }
    }

    void TxOpServiceImpl::WriteLockCallBack(::google::protobuf::RpcController* controller,
                                            const ::azino::txindex::WriteLockRequest* request,
                                            ::azino::txindex::WriteLockResponse* response,
                                            ::google::protobuf::Closure* done,
                                            void* arg) {
        // TODO use arg to decide whether to try WriteLock again or not
        WriteLock(controller, request, response, done);
    }
    void TxOpServiceImpl::ReadCallBack(::google::protobuf::RpcController* controller,
                                       const ::azino::txindex::ReadRequest* request,
                                       ::azino::txindex::ReadResponse* response,
                                       ::google::protobuf::Closure* done,
                                       void* arg) {
        // TODO use arg to decide whether to try Read again or not
        Read(controller, request, response, done);
    }
}
}