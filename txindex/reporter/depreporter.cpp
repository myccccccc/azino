#include "index.h"

DEFINE_bool(enable_dep_reporter, true, "enable dependency reporter");

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

DepReporter::DepReporter(brpc::Channel* txplaner_channel)
    : _stub(txplaner_channel), _deps_queue() {
    bthread::ExecutionQueueOptions options;
    if (bthread::execution_queue_start(&_deps_queue, &options,
                                       DepReporter::execute, this) != 0) {
        LOG(ERROR) << "fail to start execution queue in DepReporter";
    }
}

DepReporter::~DepReporter() {
    if (bthread::execution_queue_stop(_deps_queue) != 0) {
        LOG(ERROR) << "fail to stop execution queue in DepReporter";
    }
    if (bthread::execution_queue_join(_deps_queue) != 0) {
        LOG(ERROR) << "fail to join execution queue in DepReporter";
    }
}

int DepReporter::execute(void* args, bthread::TaskIterator<Deps>& iter) {
    auto p = reinterpret_cast<DepReporter*>(args);
    Deps deps;
    if (iter.is_queue_stopped()) {
        return 0;
    }
    for (; iter; ++iter) {
        auto& dep = *iter;
        deps.insert(deps.end(), dep.begin(), dep.end());
    }

    for (auto& dep : deps) {
        auto& key = dep.key;
        auto& t1 = dep.t1;
        auto& t2 = dep.t2;
        if (t1.start_ts() == t2.start_ts()) {
            continue;
        }
        LOG(INFO) << " Dep report type:"
                  << "readwrite"
                  << " key:" << key << " ts:" << t1.ShortDebugString()
                  << " t2:" << t2.ShortDebugString();
        brpc::Controller* cntl = new brpc::Controller();
        azino::txplanner::DepRequest req;
        req.set_key(key);
        req.set_allocated_t1(new TxIdentifier(t1));
        req.set_allocated_t2(new TxIdentifier(t2));
        txplanner::DepResponse* resp = new azino::txplanner::DepResponse();
        google::protobuf::Closure* done =
            brpc::NewCallback(&HandleDepResponse, cntl, resp);
        p->_stub.RWDep(cntl, &req, resp, done);
    }
    return 0;
}

void DepReporter::AsyncReadWriteReport(const Deps& deps) {
    if (deps.empty()) {
        return;
    }
    if (bthread::execution_queue_execute(_deps_queue, deps) != 0) {
        LOG(ERROR) << "fail to add task execution queue in DepReporter";
    }
}

}  // namespace txindex
}  // namespace azino