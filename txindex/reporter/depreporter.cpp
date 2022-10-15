#include "reporter.h"

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

DepReporter::DepReporter(const std::string& txplanner_addr) {
    brpc::ChannelOptions option;
    if (_channel.Init(txplanner_addr.c_str(), &option) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
    }
    _stub.reset(new txplanner::DependenceService_Stub(&_channel));
}

void DepReporter::ReadWriteReport(const std::string key, uint64_t ts1,
                                  uint64_t ts2) {
    LOG(INFO) << " Dep report type:"
              << "readwrite"
              << " key:" << key << " ts1:" << ts1 << " ts2:" << ts2
              << " ignore:" << (ts1 == ts2);
    if (ts1 == ts2) {
        return;
    }

    brpc::Controller* cntl = new brpc::Controller();
    azino::txplanner::DepRequest req;
    req.set_key(key);
    req.set_tx1_ts(ts1);
    req.set_tx2_ts(ts2);
    txplanner::DepResponse* resp = new azino::txplanner::DepResponse();
    google::protobuf::Closure* done =
        brpc::NewCallback(&HandleDepResponse, cntl, resp);
    _stub->RWDep(cntl, &req, resp, done);
}

void DepReporter::WriteWriteReport(const std::string key, uint64_t ts1,
                                   uint64_t ts2) {
    LOG(INFO) << " Dep report type:"
              << "writewrite"
              << " key:" << key << " ts1:" << ts1 << " ts2:" << ts2
              << " ignore:" << (ts1 == ts2);
    if (ts1 == ts2) {
        return;
    }

    brpc::Controller* cntl = new brpc::Controller();
    azino::txplanner::DepRequest req;
    req.set_key(key);
    req.set_tx1_ts(ts1);
    req.set_tx2_ts(ts2);
    txplanner::DepResponse* resp = new azino::txplanner::DepResponse();
    google::protobuf::Closure* done =
        brpc::NewCallback(&HandleDepResponse, cntl, resp);
    _stub->WWDep(cntl, &req, resp, done);
}

void DepReporter::WriteReadReport(const std::string key, uint64_t ts1,
                                  uint64_t ts2) {
    LOG(INFO) << " Dep report type:"
              << "writeread"
              << " key:" << key << " ts1:" << ts1 << " ts2:" << ts2
              << " ignore:" << (ts1 == ts2);
    if (ts1 == ts2) {
        return;
    }

    brpc::Controller* cntl = new brpc::Controller();
    azino::txplanner::DepRequest req;
    req.set_key(key);
    req.set_tx1_ts(ts1);
    req.set_tx2_ts(ts2);
    txplanner::DepResponse* resp = new azino::txplanner::DepResponse();
    google::protobuf::Closure* done =
        brpc::NewCallback(&HandleDepResponse, cntl, resp);
    _stub->WRDep(cntl, &req, resp, done);
}

}  // namespace txindex
}  // namespace azino