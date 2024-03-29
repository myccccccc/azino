#include <brpc/server.h>
#include <bthread/mutex.h>

#include "azino/kv.h"
#include "service.h"

namespace azino {
namespace txplanner {

RegionServiceImpl::RegionServiceImpl(TxIDTable* tt, CCPlanner* plr)
    : _tt(tt), _plr(plr) {}

RegionServiceImpl::~RegionServiceImpl() {}

void RegionServiceImpl::RWDep(::google::protobuf::RpcController* controller,
                              const ::azino::txplanner::DepRequest* request,
                              ::azino::txplanner::DepResponse* response,
                              ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    std::vector<std::pair<TxIDPtr, TxIDPtr>> v;

    for (auto& dep : request->deps()) {
        auto p1_p2 = _tt->AddDep(DepType::READWRITE, dep.t1(), dep.t2());
        if (p1_p2.first == nullptr || p1_p2.second == nullptr) {
            continue;
        }

        LOG(INFO) << cntl->remote_side() << " Dep report type:"
                  << "readwrite"
                  << " key:" << dep.key()
                  << " t1:" << dep.t1().ShortDebugString()
                  << " t2:" << dep.t2().ShortDebugString();
        v.push_back(p1_p2);
    }

    done_guard.release()->Run();

    for (auto p1_p2 : v) {
        auto abort_set = _tt->FindAbortTxnOnConsecutiveRWDep(p1_p2.first);
        for (auto p : abort_set) {
            LOG(INFO) << " tx: " << p->get_txid().ShortDebugString()
                      << " will be abort.";
            _tt->AbortTx(p->get_txid());
        }
        abort_set = _tt->FindAbortTxnOnConsecutiveRWDep(p1_p2.second);
        for (auto p : abort_set) {
            LOG(INFO) << " tx: " << p->get_txid().ShortDebugString()
                      << " will be abort.";
            _tt->AbortTx(p->get_txid());
        }
    }
}

void RegionServiceImpl::GetMinATS(
    ::google::protobuf::RpcController* controller,
    const ::azino::txplanner::GetMinATSRequest* request,
    ::azino::txplanner::GetMinATSResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    //    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    response->set_min_ats(_tt->GetMinATS());
}

void RegionServiceImpl::RegionMetric(
    ::google::protobuf::RpcController* controller,
    const ::azino::txplanner::RegionMetricRequest* request,
    ::azino::txplanner::RegionMetricResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    auto range = Range::FromPB(request->range());
    _plr->ReportMetric(range, request->metric());

    LOG(INFO) << cntl->remote_side() << " Region range:" << range.Describe()
              << " metric:" << request->metric().ShortDebugString();
}

}  // namespace txplanner
}  // namespace azino