#include "index.h"

DEFINE_bool(enable_dep_reporter, false, "enable dependency reporter");
static bvar::GFlag gflag_enable_dep_reporter("enable_dep_reporter");

namespace azino {
namespace txindex {

void HandleDepResponse(brpc::Controller* cntl, txplanner::DepResponse* resp) {
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    std::unique_ptr<txplanner::DepResponse> response_guard(resp);

    if (cntl->Failed()) {
        LOG(WARNING) << "Fail to send dep report, " << cntl->ErrorText();
        return;
    }

    if (resp->error_code() != 0) {
        LOG(WARNING) << "Fail to send dep report, error code:"
                     << resp->error_code();
    }
}

Dependence::Dependence(KVRegion* region, brpc::Channel* txplaner_channel)
    : _region(region), _stub(txplaner_channel), _deps_queue() {
    bthread::ExecutionQueueOptions options;
    if (bthread::execution_queue_start(&_deps_queue, &options,
                                       Dependence::execute, this) != 0) {
        LOG(ERROR) << "fail to start execution queue in Dependence";
    }
}

Dependence::~Dependence() {
    if (bthread::execution_queue_stop(_deps_queue) != 0) {
        LOG(ERROR) << "fail to stop execution queue in Dependence";
    }
    if (bthread::execution_queue_join(_deps_queue) != 0) {
        LOG(ERROR) << "fail to join execution queue in Dependence";
    }
}

int Dependence::execute(void* args, bthread::TaskIterator<Deps>& iter) {
    auto p = reinterpret_cast<Dependence*>(args);
    Deps deps;
    if (iter.is_queue_stopped()) {
        return 0;
    }
    for (; iter; ++iter) {
        auto& dep = *iter;
        deps.insert(deps.end(), dep.begin(), dep.end());
    }

    brpc::Controller* cntl = new brpc::Controller();
    azino::txplanner::DepRequest req;
    txplanner::DepResponse* resp = new azino::txplanner::DepResponse();
    google::protobuf::Closure* done =
        brpc::NewCallback(&HandleDepResponse, cntl, resp);

    for (auto& dep : deps) {
        auto& key = dep.key;
        auto& t1 = dep.t1;
        auto& t2 = dep.t2;

        if (t1.start_ts() == t2.start_ts()) {
            continue;
        }

        LOG(INFO) << " Dep report type: readwirte region:"
                  << p->_region->Describe() << " key:" << key
                  << " ts:" << t1.ShortDebugString()
                  << " t2:" << t2.ShortDebugString();

        auto pb_dep = req.add_deps();
        pb_dep->set_key(key);
        pb_dep->set_allocated_t1(new TxIdentifier(t1));
        pb_dep->set_allocated_t2(new TxIdentifier(t2));
    }

    p->_stub.RWDep(cntl, &req, resp, done);

    return 0;
}

void Dependence::AsyncReadWriteReport(const Deps& deps) {
    if (deps.empty()) {
        return;
    }
    if (bthread::execution_queue_execute(_deps_queue, deps) != 0) {
        LOG(ERROR) << "fail to add task execution queue in Dependence";
    }
}

}  // namespace txindex
}  // namespace azino