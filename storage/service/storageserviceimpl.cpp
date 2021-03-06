#include <butil/logging.h>
#include <brpc/server.h>

#include "service.h"

DEFINE_string(storage_name, "azino_storage", "default name of azino's storage(leveldb)");

namespace azino {
namespace storage {

    StorageServiceImpl::StorageServiceImpl() : _storage(Storage::DefaultStorage()) {
        StorageStatus ss = _storage->Open(FLAGS_storage_name);
        if (ss.error_code() != StorageStatus::Ok) {
            LOG(FATAL) << "Fail to open storage: " << FLAGS_storage_name
                       << ", error text: " << ss.error_message();
        } else {
            LOG(INFO) << "Successes to open storage: " << FLAGS_storage_name;
        }
    }

    void StorageServiceImpl::Put(::google::protobuf::RpcController* controller,
                     const ::azino::storage::PutRequest* request,
                     ::azino::storage::PutResponse* response,
                     ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

        StorageStatus ss = _storage->Put(request->key(), request->value());
        if (ss.error_code() != StorageStatus::Ok) {
            StorageStatus *ssts = new StorageStatus(ss);
            response->set_allocated_status(ssts);
            LOG(WARNING) << cntl->remote_side() << " Fail to put key: " << request->key()
                         << " error code: " << ss.error_code()
                         << " value: " << request->value() << " error message: " << ss.error_message();
        } else {
            LOG(INFO) << cntl->remote_side() << " Success to put key: " << request->key()
                      << " value: " << request->value();
        }
    }

    void StorageServiceImpl::Get(::google::protobuf::RpcController* controller,
                     const ::azino::storage::GetRequest* request,
                     ::azino::storage::GetResponse* response,
                     ::google::protobuf::Closure* done) {
            brpc::ClosureGuard done_guard(done);
            brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

            std::string value;
            StorageStatus ss = _storage->Get(request->key(), value);
            if (ss.error_code() != StorageStatus::Ok) {
                StorageStatus* ssts = new StorageStatus(ss);
                response->set_allocated_status(ssts);
                LOG(WARNING) << cntl->remote_side() << " Fail to get key: " << request->key()
                             << " error code: " << ss.error_code()
                             << " error message: " << ss.error_message();
            } else {
                LOG(INFO) << cntl->remote_side() << " Success to get key: " << request->key()
                          << " value: " << value;
                response->set_value(value);
            }
    }

    void StorageServiceImpl::Delete(::google::protobuf::RpcController* controller,
                        const ::azino::storage::DeleteRequest* request,
                        ::azino::storage::DeleteResponse* response,
                        ::google::protobuf::Closure* done) {
            brpc::ClosureGuard done_guard(done);
            brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

            StorageStatus ss = _storage->Delete(request->key());
            if (ss.error_code() != StorageStatus::Ok) {
                StorageStatus* ssts = new StorageStatus(ss);
                response->set_allocated_status(ssts);
                LOG(WARNING) << cntl->remote_side() << " Fail to delete key: " << request->key()
                             << " error code: " << ss.error_code()
                             << " error message: " << ss.error_message();
            } else {
                LOG(INFO) << cntl->remote_side() << " Success to delete key: " << request->key();
            }
    }


    void StorageServiceImpl::MVCCPut(::google::protobuf::RpcController *controller,
                                     const ::azino::storage::MVCCPutRequest *request,
                                     ::azino::storage::MVCCPutResponse *response, ::google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

        StorageStatus ss = _storage->MVCCPut(request->key(),request->ts(), request->value());
        if (ss.error_code() != StorageStatus::Ok) {
            StorageStatus *ssts = new StorageStatus(ss);
            response->set_allocated_status(ssts);
            LOG(WARNING) << cntl->remote_side() << " Fail to put mvcc key: " << request->key()
                         << " ts: " << request->ts()
                         << " error code: " << ss.error_code()
                         << " value: " << request->value() << " error message: " << ss.error_message();
        } else {
            LOG(INFO) << cntl->remote_side() << " Success to put mvcc key: " << request->key()
                      << " ts: " << request->ts()
                      << " value: " << request->value();
        }
    }

    void StorageServiceImpl::MVCCGet(::google::protobuf::RpcController *controller,
                                     const ::azino::storage::MVCCGetRequest *request,
                                     ::azino::storage::MVCCGetResponse *response, ::google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

        std::string value;
        TimeStamp  ts;
        StorageStatus ss = _storage->MVCCGet(request->key(), request->ts(), value, ts);
        if (ss.error_code() == StorageStatus_Code_Ok || ss.error_code() == StorageStatus_Code_NotFound) {
            LOG(INFO) << cntl->remote_side() << ss.error_message();
            StorageStatus* ssts = new StorageStatus(ss);
            response->set_allocated_status(ssts);
            response->set_ts(ts);
            if (ss.error_code() == StorageStatus_Code_Ok) response->set_value(value);
        } else {
            LOG(WARNING) << cntl->remote_side() << ss.error_message();
            StorageStatus* ssts = new StorageStatus(ss);
            response->set_allocated_status(ssts);
        }
    }

    void StorageServiceImpl::MVCCDelete(::google::protobuf::RpcController *controller,
                                        const ::azino::storage::MVCCDeleteRequest *request,
                                        ::azino::storage::MVCCDeleteResponse *response,
                                        ::google::protobuf::Closure *done){
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

        StorageStatus ss = _storage->MVCCDelete(request->key(),request->ts());
        if (ss.error_code() != StorageStatus::Ok) {
            StorageStatus* ssts = new StorageStatus(ss);
            response->set_allocated_status(ssts);
            LOG(WARNING) << cntl->remote_side() << " Fail to delete mvcc key: " << request->key()
                         << " ts: " << request->ts()
                         << " error code: " << ss.error_code()
                         << " error message: " << ss.error_message();
        } else {
            LOG(INFO) << cntl->remote_side() << " Success to delete mvcc key: " << request->key()
                      << " ts: " << request->ts();
        }
    }

    void StorageServiceImpl::BatchStore(::google::protobuf::RpcController* controller,
                                const ::azino::storage::BatchStoreRequest* request,
                                ::azino::storage::BatchStoreResponse* response,
                                ::google::protobuf::Closure* done) {

        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

        std::vector<Storage::Data>datas;
        datas.reserve(request->datas_size());
        for (auto &d: request->datas()) {
            datas.push_back({d.key(), d.value().content(), d.ts(), d.value().is_delete()});
        }
        StorageStatus ss = _storage->BatchStore(datas);
        if (ss.error_code() != StorageStatus::Ok) {
            StorageStatus *ssts = new StorageStatus(ss);
            response->set_allocated_status(ssts);
            LOG(WARNING) << cntl->remote_side() << " Fail to batch store mvcc data. "
                         << " error code: " << ss.error_code()
                         << " data size: " << request->datas().size() << " error message: " << ss.error_message();
        } else {
            LOG(INFO) << cntl->remote_side() << " Success to batch store mvcc data. "
                      << " data size: " << request->datas().size();
        }
    }

} // namespace storage
} // namespace azino
