#include <brpc/server.h>
#include <butil/logging.h>

#include "service.h"

DEFINE_string(storage_path, "azino_storage",
              "default name of azino's storage(leveldb)");

namespace azino {
namespace storage {

StorageServiceImpl::StorageServiceImpl() : _storage(Storage::DefaultStorage()) {
    StorageStatus ss = _storage->Open(FLAGS_storage_path);
    if (ss.error_code() != StorageStatus::Ok) {
        LOG(FATAL) << " Fail to open storage: " << FLAGS_storage_path
                   << " error code: " << ss.error_code()
                   << " error text: " << ss.error_message();
    } else {
        LOG(INFO) << " Successes to open storage: " << FLAGS_storage_path;
    }
}

void StorageServiceImpl::Put(::google::protobuf::RpcController* controller,
                             const ::azino::storage::PutRequest* request,
                             ::azino::storage::PutResponse* response,
                             ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    StorageStatus ss = _storage->Put(request->key(), request->value());
    StorageStatus* ssts = new StorageStatus(ss);
    response->set_allocated_status(ssts);
}

void StorageServiceImpl::Get(::google::protobuf::RpcController* controller,
                             const ::azino::storage::GetRequest* request,
                             ::azino::storage::GetResponse* response,
                             ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    std::string value;
    StorageStatus ss = _storage->Get(request->key(), value);
    StorageStatus* ssts = new StorageStatus(ss);
    response->set_allocated_status(ssts);
    response->set_value(value);
}

void StorageServiceImpl::Delete(::google::protobuf::RpcController* controller,
                                const ::azino::storage::DeleteRequest* request,
                                ::azino::storage::DeleteResponse* response,
                                ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    StorageStatus ss = _storage->Delete(request->key());
    StorageStatus* ssts = new StorageStatus(ss);
    response->set_allocated_status(ssts);
}

void StorageServiceImpl::MVCCPut(
    ::google::protobuf::RpcController* controller,
    const ::azino::storage::MVCCPutRequest* request,
    ::azino::storage::MVCCPutResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    StorageStatus ss =
        _storage->MVCCPut(request->key(), request->ts(), request->value());
    StorageStatus* ssts = new StorageStatus(ss);
    response->set_allocated_status(ssts);

    LOG(INFO) << " MVCCPUT remote side: " << cntl->remote_side()
              << " request: " << request->ShortDebugString()
              << " error code: " << ss.error_code()
              << " error message: " << ss.error_message();
}

void StorageServiceImpl::MVCCGet(
    ::google::protobuf::RpcController* controller,
    const ::azino::storage::MVCCGetRequest* request,
    ::azino::storage::MVCCGetResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    std::string value;
    TimeStamp ts;
    StorageStatus ss =
        _storage->MVCCGet(request->key(), request->ts(), value, ts);
    StorageStatus* ssts = new StorageStatus(ss);
    response->set_allocated_status(ssts);
    response->set_ts(ts);
    response->set_value(value);

    LOG(INFO) << " MVCCGET remote side: " << cntl->remote_side()
              << " request: " << request->ShortDebugString()
              << " error code: " << ss.error_code()
              << " error message: " << ss.error_message();
}

void StorageServiceImpl::MVCCDelete(
    ::google::protobuf::RpcController* controller,
    const ::azino::storage::MVCCDeleteRequest* request,
    ::azino::storage::MVCCDeleteResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    StorageStatus ss = _storage->MVCCDelete(request->key(), request->ts());
    StorageStatus* ssts = new StorageStatus(ss);
    response->set_allocated_status(ssts);

    LOG(INFO) << " MVCCDELETE remote side: " << cntl->remote_side()
              << " request: " << request->ShortDebugString()
              << " error code: " << ss.error_code()
              << " error message: " << ss.error_message();
}

void StorageServiceImpl::BatchStore(
    ::google::protobuf::RpcController* controller,
    const ::azino::storage::BatchStoreRequest* request,
    ::azino::storage::BatchStoreResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    std::vector<Storage::Data> datas;
    datas.reserve(request->datas_size());
    for (auto& d : request->datas()) {
        datas.push_back(
            {d.key(), d.value().content(), d.ts(), d.value().is_delete()});
    }
    StorageStatus ss = _storage->BatchStore(datas);
    StorageStatus* ssts = new StorageStatus(ss);
    response->set_allocated_status(ssts);

    LOG(INFO) << " BATCHSTORE remote side: " << cntl->remote_side()
              << " request: " << request->ShortDebugString()
              << " error code: " << ss.error_code()
              << " error message: " << ss.error_message();
}

}  // namespace storage
}  // namespace azino
