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
    : _stub(txplaner_channel) {}

void DepReporter::AsyncReadWriteReport(const std::string& key,
                                       const std::vector<Dep>& deps) {
    if (deps.empty()) {
        return;
    }

    for (size_t i = 0; i < deps.size(); i++) {
        auto& t1 = deps[i].t1;
        auto& t2 = deps[i].t2;
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
        _stub.RWDep(cntl, &req, resp, done);
    }
}

}  // namespace txindex
}  // namespace azino