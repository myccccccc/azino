#ifndef AZINO_TXPLANNER_INCLUDE_SERVICE_H
#define AZINO_TXPLANNER_INCLUDE_SERVICE_H

#include <memory>

#include "partition_manager.h"
#include "service/tx.pb.h"
#include "service/txplanner/txplanner.pb.h"
#include "txidtable.h"

namespace azino {
namespace txplanner {
class AscendingTimer;

class TxServiceImpl : public TxService {
   public:
    TxServiceImpl(TxIDTable* tt, PartitionManager* pm);
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

    virtual void ValidateTx(
        ::google::protobuf::RpcController* controller,
        const ::azino::txplanner::ValidateTxRequest* request,
        ::azino::txplanner::ValidateTxResponse* response,
        ::google::protobuf::Closure* done) override;

   private:
    std::unique_ptr<AscendingTimer> _timer;
    TxIDTable* _tt;
    PartitionManager* _pm;
};

class RegionServiceImpl : public RegionService {
   public:
    RegionServiceImpl(TxIDTable* tt);
    ~RegionServiceImpl();

    virtual void RWDep(::google::protobuf::RpcController* controller,
                       const ::azino::txplanner::DepRequest* request,
                       ::azino::txplanner::DepResponse* response,
                       ::google::protobuf::Closure* done) override;

    virtual void GetMinATS(::google::protobuf::RpcController* controller,
                           const ::azino::txplanner::GetMinATSRequest* request,
                           ::azino::txplanner::GetMinATSResponse* response,
                           ::google::protobuf::Closure* done) override;

    virtual void RegionMetric(
        ::google::protobuf::RpcController* controller,
        const ::azino::txplanner::RegionMetricRequest* request,
        ::azino::txplanner::RegionMetricResponse* response,
        ::google::protobuf::Closure* done) override;

   private:
    TxIDTable* _tt;
};

class PartitionServiceImpl : public PartitionService {
   public:
    PartitionServiceImpl(PartitionManager* pm);
    ~PartitionServiceImpl();

    virtual void GetPartition(
        ::google::protobuf::RpcController* controller,
        const ::azino::txplanner::GetPartitionRequest* request,
        ::azino::txplanner::GetPartitionResponse* response,
        ::google::protobuf::Closure* done) override;

   private:
    PartitionManager* _pm;
};

}  // namespace txplanner
}  // namespace azino
#endif  // AZINO_TXPLANNER_INCLUDE_SERVICE_H
