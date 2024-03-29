#ifndef AZINO_TXINDEX_INCLUDE_SERVICE_H
#define AZINO_TXINDEX_INCLUDE_SERVICE_H

#include <butil/macros.h>

#include <string>

#include "service/txindex/txindex.pb.h"

namespace azino {
namespace txindex {
class Dependence;
class TxIndex;

class TxOpServiceImpl : public TxOpService {
   public:
    TxOpServiceImpl(TxIndex* index);
    DISALLOW_COPY_AND_ASSIGN(TxOpServiceImpl);
    ~TxOpServiceImpl();

    virtual void WriteIntent(
        ::google::protobuf::RpcController* controller,
        const ::azino::txindex::WriteIntentRequest* request,
        ::azino::txindex::WriteIntentResponse* response,
        ::google::protobuf::Closure* done) override;
    virtual void WriteLock(::google::protobuf::RpcController* controller,
                           const ::azino::txindex::WriteLockRequest* request,
                           ::azino::txindex::WriteLockResponse* response,
                           ::google::protobuf::Closure* done) override;
    virtual void Clean(::google::protobuf::RpcController* controller,
                       const ::azino::txindex::CleanRequest* request,
                       ::azino::txindex::CleanResponse* response,
                       ::google::protobuf::Closure* done) override;
    virtual void Commit(::google::protobuf::RpcController* controller,
                        const ::azino::txindex::CommitRequest* request,
                        ::azino::txindex::CommitResponse* response,
                        ::google::protobuf::Closure* done) override;
    virtual void Read(::google::protobuf::RpcController* controller,
                      const ::azino::txindex::ReadRequest* request,
                      ::azino::txindex::ReadResponse* response,
                      ::google::protobuf::Closure* done) override;

   private:
    TxIndex* _index;
};
}  // namespace txindex
}  // namespace azino

#endif  // AZINO_TXINDEX_INCLUDE_SERVICE_H
