#ifndef AZINO_STORAGE_INCLUDE_SERVICE_H
#define AZINO_STORAGE_INCLUDE_SERVICE_H

#include "service/storage/storage.pb.h"
#include "storage.h"

namespace azino {
namespace storage {

class StorageServiceImpl : public StorageService {
   public:
    StorageServiceImpl();

    virtual ~StorageServiceImpl() = default;

    virtual void MVCCPut(::google::protobuf::RpcController* controller,
                         const ::azino::storage::MVCCPutRequest* request,
                         ::azino::storage::MVCCPutResponse* response,
                         ::google::protobuf::Closure* done) override;

    virtual void MVCCGet(::google::protobuf::RpcController* controller,
                         const ::azino::storage::MVCCGetRequest* request,
                         ::azino::storage::MVCCGetResponse* response,
                         ::google::protobuf::Closure* done) override;

    virtual void MVCCDelete(::google::protobuf::RpcController* controller,
                            const ::azino::storage::MVCCDeleteRequest* request,
                            ::azino::storage::MVCCDeleteResponse* response,
                            ::google::protobuf::Closure* done) override;

    virtual void MVCCScan(::google::protobuf::RpcController* controller,
                          const ::azino::storage::MVCCScanRequest* request,
                          ::azino::storage::MVCCScanResponse* response,
                          ::google::protobuf::Closure* done) override;

    virtual void BatchStore(::google::protobuf::RpcController* controller,
                            const ::azino::storage::BatchStoreRequest* request,
                            ::azino::storage::BatchStoreResponse* response,
                            ::google::protobuf::Closure* done) override;

   private:
    std::unique_ptr<Storage> _storage;
};

}  // namespace storage
}  // namespace azino

#endif  // AZINO_STORAGE_INCLUDE_SERVICE_H
