#include <brpc/channel.h>
#include <butil/hash.h>

#include "azino/client.h"
#include "txwritebuffer.h"
#include "service/tx.pb.h"
#include "service/txplanner/txplanner.pb.h"
#include "service/txindex/txindex.pb.h"
#include "service/storage/storage.pb.h"

namespace azino {

DEFINE_int32(timeout_ms, -1, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 2, "Max retries(not including the first RPC)");

    Transaction::Transaction(const Options& options, const std::string& txplanner_addr)
    : _options(new Options(options)),
      _channel_options(new brpc::ChannelOptions),
      _txid(nullptr),
      _txwritebuffer(new TxWriteBuffer) {
        _channel_options->timeout_ms = FLAGS_timeout_ms;
        _channel_options->max_retry = FLAGS_max_retry;

        auto* channel = new brpc::Channel();
        if (channel->Init(txplanner_addr.c_str(), _channel_options.get()) != 0) {
            LOG(ERROR) << "Fail to initialize channel: " << txplanner_addr;
        }
        _txplanner.reset(channel);
    }

    Transaction::~Transaction() = default;

    Status Transaction::Begin() {
        std::stringstream ss;
        if (_txid) {
            ss << "Transaction has already began. " << _txid->ShortDebugString();
            return Status::IllegalTxOp(ss.str());
        }
        azino::txplanner::TxService_Stub stub(_txplanner.get());
        brpc::Controller cntl;
        azino::txplanner::BeginTxRequest req;
        azino::txplanner::BeginTxResponse resp;
        stub.BeginTx(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            ss << "Controller failed error code: " << cntl.ErrorCode() << " error text: " << cntl.ErrorText();
            LOG(WARNING) << ss.str();
            return Status::NetworkErr(ss.str());
        }
        ss << "sdk: " << cntl.local_side() << " BeginTx from txplanner: " << cntl.remote_side() << std::endl
           << "request: " << req.ShortDebugString() << std::endl
           << "response: " << resp.ShortDebugString() << std::endl
           << "latency=" << cntl.latency_us() << "us";
        if (!resp.has_storage_addr() || resp.txindex_addrs_size() == 0) {
            ss << " fail. ";
            LOG(WARNING) << ss.str();
            return Status::NotSupportedErr(ss.str());
        }
        ss << " success. ";
        LOG(INFO) << ss.str();

        _txid.reset(resp.release_txid());
        assert(_txid->status().status_code() == TxStatus_Code_Started);

        auto* channel = new brpc::Channel();
        if (channel->Init(resp.storage_addr().c_str(), _channel_options.get()) != 0) {
            LOG(ERROR) << "Fail to initialize channel: " << resp.storage_addr();
        }
        _storage.reset(channel);

        for (int i = 0; i < resp.txindex_addrs_size(); i++) {
            auto* txindex_channel = new brpc::Channel();
            if (txindex_channel->Init(resp.txindex_addrs(i).c_str(), _channel_options.get()) != 0) {
                LOG(ERROR) << "Fail to initialize channel: " << resp.txindex_addrs(i);
            }
            _txindexs.emplace_back(txindex_channel);
        }

        return Status::Ok(ss.str());
    }

    Status Transaction::Commit() {
        std::stringstream ss;
        if (!_txid) {
            ss << "Transaction has not began. ";
            return Status::IllegalTxOp(ss.str());
        }
        if (_txid->status().status_code() != TxStatus_Code_Started) {
            ss << "Transaction is not allowed to commit. " << _txid->ShortDebugString();
            return Status::IllegalTxOp(ss.str());
        }

        azino::txplanner::TxService_Stub stub(_txplanner.get());
        brpc::Controller cntl;
        azino::txplanner::CommitTxRequest req;
        req.set_allocated_txid(new TxIdentifier(*_txid));
        azino::txplanner::CommitTxResponse resp;
        stub.CommitTx(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            ss << "Controller failed error code: " << cntl.ErrorCode() << " error text: " << cntl.ErrorText();
            LOG(WARNING) << ss.str();
            return Status::NetworkErr(ss.str());
        }
        ss << "sdk: " << cntl.local_side() << " CommitTx from txplanner: " << cntl.remote_side() << std::endl
           << "request: " << req.ShortDebugString() << std::endl
           << "response: " << resp.ShortDebugString() << std::endl
           << "latency=" << cntl.latency_us() << "us";
        ss << " success. ";
        LOG(INFO) << ss.str();

        _txid.reset(resp.release_txid());
        auto* txid_sts = _txid->release_status();
        _txid->set_allocated_status(txid_sts);

        Status preput_sts = PreputAll();
        if (preput_sts.IsOk()) {
            txid_sts->set_status_code(TxStatus_Code_Committing);
            Status commit_sts = CommitAll();
            if (commit_sts.IsOk()) {
                txid_sts->set_status_code(TxStatus_Code_Committed);
                txid_sts->set_status_message(commit_sts.ToString());
                return commit_sts;
            } else {
                txid_sts->set_status_code(TxStatus_Code_Abnormal);
                txid_sts->set_status_message(commit_sts.ToString());
                return commit_sts;
            }
        } else {
            txid_sts->set_status_code(TxStatus_Code_Aborting);
            Status abort_sts =  AbortAll();
            if (abort_sts.IsOk()) {
                txid_sts->set_status_code(TxStatus_Code_Aborted);
                txid_sts->set_status_message(preput_sts.ToString());
                return preput_sts;
            } else {
                txid_sts->set_status_code(TxStatus_Code_Abnormal);
                txid_sts->set_status_message(abort_sts.ToString());
                return abort_sts;
            }
        }
    }

    Status Transaction::PreputAll() {
        assert(_txid->status().status_code() == TxStatus_Code_Preputting);
        std::stringstream ss;
        for (auto iter = _txwritebuffer->begin(); iter != _txwritebuffer->end(); iter++) {
            assert(!iter->second.preput);
            ss = std::stringstream();
            auto txindex_num = butil::Hash(iter->first) % _txindexs.size();
            azino::txindex::TxOpService_Stub stub(_txindexs[txindex_num].get());
            brpc::Controller cntl;
            azino::txindex::WriteIntentRequest req;
            req.set_allocated_txid(new TxIdentifier(*_txid));
            req.set_key(iter->first);
            req.set_allocated_value(iter->second.value.get());
            azino::txindex::WriteIntentResponse resp;
            stub.WriteIntent(&cntl, &req, &resp, nullptr);
            if (cntl.Failed()) {
                ss << "Controller failed error code: " << cntl.ErrorCode() << " error text: " << cntl.ErrorText();
                LOG(WARNING) << ss.str();
                req.release_value();
                return Status::NetworkErr(ss.str());
            }
            ss << "sdk: " << cntl.local_side() << " WriteIntent from txindex: " << cntl.remote_side() << std::endl
               << "request: " << req.ShortDebugString() << std::endl
               << "response: " << resp.ShortDebugString() << std::endl
               << "latency=" << cntl.latency_us() << "us";
            req.release_value();
            switch (resp.tx_op_status().error_code()) {
                case TxOpStatus_Code_Ok:
                    ss << " success. ";
                    LOG(INFO) << ss.str();
                    iter->second.preput = true;
                    break;
                case TxOpStatus_Code_WriteTooLate:
                case TxOpStatus_Code_WriteConflicts:
                    ss << " fail. ";
                    LOG(INFO) << ss.str();
                    return Status::TxIndexErr(ss.str());
                default:
                    ss << " fail. ";
                    LOG(ERROR) << ss.str();
                    return Status::TxIndexErr(ss.str());
            }
        }
        return Status::Ok(); // todo: add some error message
    }

    Status Transaction::CommitAll() {
        assert(_txid->status().status_code() == TxStatus_Code_Committing);
        std::stringstream ss;

        for (auto iter = _txwritebuffer->begin(); iter != _txwritebuffer->end(); iter++) {
            assert(iter->second.preput);
            ss = std::stringstream();
            auto txindex_num = butil::Hash(iter->first) % _txindexs.size();
            azino::txindex::TxOpService_Stub stub(_txindexs[txindex_num].get());
            brpc::Controller cntl;
            azino::txindex::CommitRequest req;
            req.set_allocated_txid(new TxIdentifier(*_txid));
            req.set_key(iter->first);
            azino::txindex::CommitResponse resp;
            stub.Commit(&cntl, &req, &resp, nullptr);
            if (cntl.Failed()) {
                ss << "Controller failed error code: " << cntl.ErrorCode() << " error text: " << cntl.ErrorText();
                LOG(WARNING) << ss.str();
                return Status::NetworkErr(ss.str());
            }
            ss << "sdk: " << cntl.local_side() << " Commit from txindex: " << cntl.remote_side() << std::endl
               << "request: " << req.ShortDebugString() << std::endl
               << "response: " << resp.ShortDebugString() << std::endl
               << "latency=" << cntl.latency_us() << "us";
            switch (resp.tx_op_status().error_code()) {
                case TxOpStatus_Code_Ok:
                    ss << " success. ";
                    LOG(INFO) << ss.str();
                    break;
                default:
                    ss << " fail. ";
                    LOG(ERROR) << ss.str();
                    return Status::TxIndexErr(ss.str());
            }
        }
        return Status::Ok(); // todo: add some error message
    }

    Status Transaction::AbortAll() {
        assert(_txid->status().status_code() == TxStatus_Code_Aborting);
        std::stringstream ss;

        for (auto iter = _txwritebuffer->begin(); iter != _txwritebuffer->end(); iter++) {
            if (!iter->second.preput && iter->second.options.type == kOptimistic) continue;
            ss = std::stringstream();
            auto txindex_num = butil::Hash(iter->first) % _txindexs.size();
            azino::txindex::TxOpService_Stub stub(_txindexs[txindex_num].get());
            brpc::Controller cntl;
            azino::txindex::CleanRequest req;
            req.set_allocated_txid(new TxIdentifier(*_txid));
            req.set_key(iter->first);
            azino::txindex::CleanResponse resp;
            stub.Clean(&cntl, &req, &resp, nullptr);
            if (cntl.Failed()) {
                ss << "Controller failed error code: " << cntl.ErrorCode() << " error text: " << cntl.ErrorText();
                LOG(WARNING) << ss.str();
                return Status::NetworkErr(ss.str());
            }
            ss << "sdk: " << cntl.local_side() << " Clean from txindex: " << cntl.remote_side() << std::endl
               << "request: " << req.ShortDebugString() << std::endl
               << "response: " << resp.ShortDebugString() << std::endl
               << "latency=" << cntl.latency_us() << "us";
            switch (resp.tx_op_status().error_code()) {
                case TxOpStatus_Code_Ok:
                    ss << " success. ";
                    LOG(INFO) << ss.str();
                    iter->second.preput = false;
                    break;
                default:
                    ss << " fail. ";
                    LOG(ERROR) << ss.str();
                    return Status::TxIndexErr(ss.str());
            }
        }
        return Status::Ok(); // todo: add some error message
    }

    Status Transaction::Put(const WriteOptions& options, const UserKey& key, const UserValue& value) {
        return Write(options, key, false, value);
    }

    Status Transaction::Delete(const WriteOptions& options, const UserKey& key) {
        return Write(options, key, true);
    }

    Status Transaction::Write(const WriteOptions& options, const UserKey& key, bool is_delete, const UserValue& value) {
        std::stringstream ss;
        if (!_txid) {
            ss << "Transaction has not began. ";
            return Status::IllegalTxOp(ss.str());
        }
        if (_txid->status().status_code() != TxStatus_Code_Started) {
            ss << "Transaction is not allowed to put. " << _txid->ShortDebugString();
            return Status::IllegalTxOp(ss.str());
        }

        std::unique_ptr<Value>saved_value(new Value);
        if (is_delete) {
            saved_value->set_is_delete(true);
        } else {
            saved_value->set_is_delete(false);
            saved_value->set_content(value);
        }
        WriteOptions saved_options = options;
        if (saved_options.type == WriteType::kAutomatic) {
            // todo: automatic chose type
            saved_options.type = WriteType::kOptimistic;
        }

        if (saved_options.type == kPessimistic
            && (_txwritebuffer->find(key) == _txwritebuffer->end()
                || _txwritebuffer->find(key)->second.options.type != kPessimistic)) { // Pessimistic
            auto txindex_num = butil::Hash(key) % _txindexs.size();
            azino::txindex::TxOpService_Stub stub(_txindexs[txindex_num].get());
            brpc::Controller cntl;
            azino::txindex::WriteLockRequest req;
            req.set_key(key);
            req.set_allocated_txid(new TxIdentifier(*_txid));
            azino::txindex::WriteLockResponse resp;
            stub.WriteLock(&cntl, &req, &resp, nullptr);
            if (cntl.Failed()) {
                ss << "Controller failed error code: " << cntl.ErrorCode() << " error text: " << cntl.ErrorText();
                LOG(WARNING) << ss.str();
                return Status::NetworkErr(ss.str());
            }
            ss << "sdk: " << cntl.local_side() << " WriteLock from txindex: " << cntl.remote_side() << std::endl
               << "request: " << req.ShortDebugString() << std::endl
               << "response: " << resp.ShortDebugString() << std::endl
               << "latency=" << cntl.latency_us() << "us";
            switch (resp.tx_op_status().error_code()) {
                case TxOpStatus_Code_Ok:
                    ss << " success. ";
                    LOG(INFO) << ss.str();
                    break;
                case TxOpStatus_Code_WriteTooLate:
                    // todo: fail to lock, may use some optimistic approach
                    ss << " fail. ";
                    LOG(INFO) << ss.str();
                    return Status::TxIndexErr(ss.str());
                default:
                    ss << " fail. ";
                    LOG(ERROR) << ss.str();
                    return Status::TxIndexErr(ss.str());
            }
        }

        ss << "Write in TxWriteBuffer key: " << key << " Value: " << saved_value->ShortDebugString();
        _txwritebuffer->Write(key, saved_value.release(), saved_options);
        return Status::Ok(ss.str());
    }

    Status Transaction::Get(const ReadOptions& options, const UserKey& key, UserValue& value) {
        std::stringstream ss;
        auto iter = _txwritebuffer->find(key);
        if (iter != _txwritebuffer->end()) {
            auto v = iter->second.value;
            ss << "Find in TxWriteBuffer Key: " << key << " Value: " << v->ShortDebugString();
            if (v->is_delete()) {
                return Status::NotFound(ss.str());
            } else {
                value = v->content();
                return Status::Ok(ss.str());
            }
        }
        auto txindex_num = butil::Hash(key) % _txindexs.size();
        azino::txindex::TxOpService_Stub stub(_txindexs[txindex_num].get());
        brpc::Controller cntl;
        azino::txindex::ReadRequest req;
        req.set_key(key);
        req.set_allocated_txid(new TxIdentifier(*_txid));
        azino::txindex::ReadResponse resp;
        stub.Read(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            ss << "Controller failed error code: " << cntl.ErrorCode() << " error text: " << cntl.ErrorText();
            LOG(WARNING) << ss.str();
            return Status::NetworkErr(ss.str());
        }
        ss << "sdk: " << cntl.local_side() << " Read from txindex: " << cntl.remote_side() << std::endl
           << "request: " << req.ShortDebugString() << std::endl
           << "response: " << resp.ShortDebugString() << std::endl
           << "latency=" << cntl.latency_us() << "us";
        switch (resp.tx_op_status().error_code()) {
            case TxOpStatus_Code_Ok:
                ss << " success. ";
                LOG(INFO) << ss.str();
                if (resp.value().is_delete()) {
                    return Status::NotFound(ss.str());
                } else {
                    value = resp.value().content();
                    return Status::Ok(ss.str());
                }
            case TxOpStatus_Code_ReadNotExist:
                ss << " fail. ";
                LOG(INFO) << ss.str();
                goto readStorage;
            default:
                ss << " fail. ";
                LOG(ERROR) << ss.str();
                return Status::TxIndexErr(ss.str());
        }
        
        readStorage:
        azino::storage::StorageService_Stub storage_stub(_storage.get());
        brpc::Controller storage_cntl;
        azino::storage::MVCCGetRequest storage_req;
        storage_req.set_key(key);
        storage_req.set_ts(_txid->start_ts());
        azino::storage::MVCCGetResponse storage_resp;
        storage_stub.MVCCGet(&storage_cntl, &storage_req, &storage_resp, nullptr);
        std::stringstream storage_ss;
        if (storage_cntl.Failed()) {
            storage_ss << "Controller failed error code: " << storage_cntl.ErrorCode() << " error text: " << storage_cntl.ErrorText();
            LOG(WARNING) << storage_ss.str();
            return Status::NetworkErr(storage_ss.str());
        }
        storage_ss << "sdk: " << storage_cntl.local_side() << " Read from storage: " << storage_cntl.remote_side() << std::endl
           << "request: " << storage_req.ShortDebugString() << std::endl
           << "response: " << storage_resp.ShortDebugString() << std::endl
           << "latency=" << storage_cntl.latency_us() << "us";
        switch (storage_resp.status().error_code()) {
            case storage::StorageStatus_Code_Ok:
                storage_ss << " success. ";
                LOG(INFO) << storage_ss.str();
                value = storage_resp.value();
                return Status::Ok(storage_ss.str());
            case storage::StorageStatus_Code_NotFound:
                storage_ss << " fail. ";
                LOG(INFO) << storage_ss.str();
                return Status::NotFound(storage_ss.str());
            default:
                storage_ss << " fail. ";
                LOG(ERROR) << storage_ss.str();
                return Status::StorageErr(storage_ss.str());
        }
    }
}