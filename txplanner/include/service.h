#ifndef AZINO_TXPLANNER_INCLUDE_SERVICE_H
#define AZINO_TXPLANNER_INCLUDE_SERVICE_H

#include <memory>

#include "service/tx.pb.h"
#include "service/txplanner/txplanner.pb.h"
#include "txidtable.h"

namespace azino {
namespace txplanner {
class AscendingTimer;

class TxServiceImpl : public TxService {
   public:
    TxServiceImpl(const std::vector<std::string>& txindex_addrs,
                  const std::string& storage_addr, TxIDTable* tt);
    ~TxServiceImpl();

    virtual void BeginTx(::google::protobuf::RpcController* controller,
                         const ::azino::txplanner::BeginTxRequest* request,
                         ::azino::txplanner::BeginTxResponse* response,
                         ::google::protobuf::Closure* done) override;

    virtual void CommitTx(::google::protobuf::RpcController* controller,
                          const ::azino::txplanner::CommitTxRequest* request,
                          ::azino::txplanner::CommitTxResponse* response,
                          ::google::protobuf::Closure* done) override;

    virtual void AbortTx(::google::protobuf::RpcController* controller,
                         const ::azino::txplanner::AbortTxRequest* request,
                         ::azino::txplanner::AbortTxResponse* response,
                         ::google::protobuf::Closure* done) override;

   private:
    std::unique_ptr<AscendingTimer> _timer;
    TxIDTable* _tt;
    std::vector<std::string>
        _txindex_addrs;         // txindex addresses in form of "0.0.0.0:8000"
    std::string _storage_addr;  // storage addresses in form of "0.0.0.0:8000"
};

class DependenceServiceImpl : public DependenceService {
   public:
    DependenceServiceImpl(TxIDTable* tt);
    ~DependenceServiceImpl();

    virtual void RWDep(::google::protobuf::RpcController* controller,
                       const ::azino::txplanner::DepRequest* request,
                       ::azino::txplanner::DepResponse* response,
                       ::google::protobuf::Closure* done) override;

   private:
    TxIDTable* _tt;
};

}  // namespace txplanner
}  // namespace azino
#endif  // AZINO_TXPLANNER_INCLUDE_SERVICE_H
