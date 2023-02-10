#include "azino/client.h"

#include <brpc/channel.h>
#include <butil/hash.h>

#include "service/storage/storage.pb.h"
#include "service/tx.pb.h"
#include "service/txindex/txindex.pb.h"
#include "service/txplanner/txplanner.pb.h"
#include "txwritebuffer.h"

namespace azino {

DEFINE_int32(timeout_ms, -1, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 2, "Max retries(not including the first RPC)");

#define LOG_CHANNEL_ERROR(addr, err, ss)                                     \
    ss << " Fail to initialize channel: " << addr << " error code: " << err; \
    LOG(WARNING) << ss.str();

#define LOG_CONTROLLER_ERROR(cntl, ss)                              \
    ss << " Sdk controller failed error code: " << cntl.ErrorCode() \
       << " error text: " << cntl.ErrorText();                      \
    LOG(WARNING) << ss.str();

#define LOG_SDK(cntl, req, resp, msg)                                         \
    LOG(INFO) << " Sdk: " << cntl.local_side() << " " << #msg << ": "         \
              << cntl.remote_side() << " request: " << req.ShortDebugString() \
              << " response: " << resp.ShortDebugString()                     \
              << " latency= " << cntl.latency_us() << "us" << std::endl;

Transaction::Transaction(const Options& options,
                         const std::string& txplanner_addr)
    : _options(options),
      _channel_options(new brpc::ChannelOptions),
      _txid(nullptr),
      _txwritebuffer(new TxWriteBuffer) {
    _channel_options->timeout_ms = FLAGS_timeout_ms;
    _channel_options->max_retry = FLAGS_max_retry;

    std::stringstream ss;
    auto* channel = new brpc::Channel();
    int err;
    err = channel->Init(txplanner_addr.c_str(), _channel_options.get());
    if (err) {
        LOG_CHANNEL_ERROR(txplanner_addr, err, ss)
        return;
    }
    _txplanner.reset(channel);
}

Transaction::~Transaction() = default;

Status Transaction::Begin() {
    int err = 0;
    std::stringstream ss;
    if (_txid) {
        ss << " Transaction has already began. " << _txid->ShortDebugString();
        return Status::IllegalTxOp(ss.str());
    }
    azino::txplanner::TxService_Stub stub(_txplanner.get());
    brpc::Controller cntl;
    azino::txplanner::BeginTxRequest req;
    azino::txplanner::BeginTxResponse resp;
    stub.BeginTx(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        LOG_CONTROLLER_ERROR(cntl, ss)
        return Status::NetworkErr(ss.str());
    }

    LOG_SDK(cntl, req, resp, BeginTx_from_txplanner)

    _txid.reset(resp.release_txid());
    if (_txid->status().status_code() != TxStatus_Code_Start) {
        ss << " Wrong tx status code: " << _txid->status().status_code();
        return Status::TxPlannerErr(ss.str());
    }
    if (!resp.has_storage_addr()) {
        ss << " Missing storage addr";
        return Status::TxPlannerErr(ss.str());
    }
    if (resp.txindex_addrs_size() == 0) {
        ss << " Missing txindex addrs";
        return Status::TxPlannerErr(ss.str());
    }

    auto* channel = new brpc::Channel();
    err = channel->Init(resp.storage_addr().c_str(), _channel_options.get());
    if (err) {
        LOG_CHANNEL_ERROR(resp.storage_addr(), err, ss)
        return Status::NetworkErr(ss.str());
    }
    _storage.reset(channel);

    for (int i = 0; i < resp.txindex_addrs_size(); i++) {
        auto* txindex_channel = new brpc::Channel();
        err = txindex_channel->Init(resp.txindex_addrs(i).c_str(),
                                    _channel_options.get());
        if (err) {
            LOG_CHANNEL_ERROR(resp.txindex_addrs(i), err, ss)
            return Status::NetworkErr(ss.str());
        }
        _txindexs.emplace_back(txindex_channel);
    }

    return Status::Ok();
}

Status Transaction::Commit() {
    std::stringstream ss;
    Status preput_sts = Status::Ok();
    Status commit_sts = Status::Ok();
    Status abort_sts = Status::Ok();
    azino::txplanner::TxService_Stub stub(_txplanner.get());
    brpc::Controller cntl;
    azino::txplanner::CommitTxRequest req;
    azino::txplanner::CommitTxResponse resp;
    azino::txplanner::AbortTxRequest areq;
    azino::txplanner::AbortTxResponse aresp;
    if (!_txid) {
        ss << " Transaction has not began. ";
        return Status::IllegalTxOp(ss.str());
    }
    if (_txid->status().status_code() != TxStatus_Code_Start) {
        ss << " Transaction is not allowed to commit. "
           << _txid->ShortDebugString();
        return Status::IllegalTxOp(ss.str());
    }

    auto txid_sts = _txid->release_status();
    txid_sts->set_status_code(TxStatus_Code_Preput);
    _txid->set_allocated_status(txid_sts);

    preput_sts = PreputAll();
    if (!preput_sts.IsOk()) {
        goto abort;
    }

    req.set_allocated_txid(new TxIdentifier(*_txid));
    stub.CommitTx(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        LOG_CONTROLLER_ERROR(cntl, ss)
        return Status::NetworkErr(ss.str());
    }

    LOG_SDK(cntl, req, resp, CommitTx_from_txplanner)

    _txid.reset(resp.release_txid());
    txid_sts = _txid->release_status();
    _txid->set_allocated_status(txid_sts);
    if (_txid->status().status_code() != TxStatus_Code_Commit) {
        ss << " Wrong tx status code when commit: "
           << _txid->ShortDebugString();
        preput_sts = Status::TxPlannerErr(ss.str());
        goto abort;
    }

    commit_sts = CommitAll();
    if (commit_sts.IsOk()) {
        txid_sts->set_status_message(commit_sts.ToString());
        return commit_sts;
    } else {
        txid_sts->set_status_code(TxStatus_Code_Abnormal);
        txid_sts->set_status_message(commit_sts.ToString());
        return commit_sts;
    }

abort:
    if (_txid->status().status_code() != TxStatus_Code_Abort) {
        areq.set_allocated_txid(new TxIdentifier(*_txid));
        stub.AbortTx(&cntl, &areq, &aresp, nullptr);
        if (cntl.Failed()) {
            LOG_CONTROLLER_ERROR(cntl, ss)
            return Status::NetworkErr(ss.str());
        }

        LOG_SDK(cntl, areq, aresp, AbortTx_from_txplanner)

        _txid.reset(aresp.release_txid());
    }

    txid_sts = _txid->release_status();
    _txid->set_allocated_status(txid_sts);
    if (_txid->status().status_code() != TxStatus_Code_Abort) {
        ss << " Wrong tx status code when abort: " << _txid->ShortDebugString();
        return Status::TxPlannerErr(ss.str());
    }

    abort_sts = AbortAll();
    if (abort_sts.IsOk()) {
        txid_sts->set_status_message(preput_sts.ToString());
        return preput_sts;
    } else {
        txid_sts->set_status_code(TxStatus_Code_Abnormal);
        txid_sts->set_status_message(abort_sts.ToString());
        return abort_sts;
    }
}

Status Transaction::PreputAll() {
    std::stringstream ss;

    // todo: parallelize preputing
    for (auto iter = _txwritebuffer->begin(); iter != _txwritebuffer->end();
         iter++) {
        assert(iter->second.status < TxWriteStatus::PREPUTED);

        // todo: use a wrapper
        auto txindex_num = butil::Hash(iter->first) % _txindexs.size();
        azino::txindex::TxOpService_Stub stub(_txindexs[txindex_num].get());

        brpc::Controller cntl;
        azino::txindex::WriteIntentRequest req;
        // todo: avoid memory copy here
        req.set_allocated_txid(new TxIdentifier(*_txid));
        req.set_key(iter->first);
        req.set_allocated_value(&iter->second.value);
        azino::txindex::WriteIntentResponse resp;
        stub.WriteIntent(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            LOG_CONTROLLER_ERROR(cntl, ss)
            req.release_value();
            return Status::NetworkErr(ss.str());
        }

        LOG_SDK(cntl, req, resp, WriteIntent_from_txindex)

        req.release_value();

        switch (resp.tx_op_status().error_code()) {
            case TxOpStatus_Code_Ok:
                iter->second.status = TxWriteStatus::PREPUTED;
                break;
            default:
                ss << " Preput key: " << iter->first
                   << " value: " << iter->second.value.ShortDebugString()
                   << " error code: " << resp.tx_op_status().error_code()
                   << " error message: " << resp.tx_op_status().error_message();
                return Status::TxIndexErr(ss.str());
        }
    }

    return Status::Ok();
}

Status Transaction::CommitAll() {
    std::stringstream ss;

    // todo: parallelize committing
    for (auto iter = _txwritebuffer->begin(); iter != _txwritebuffer->end();
         iter++) {
        assert(iter->second.status == TxWriteStatus::PREPUTED);

        // todo: use a wrapper
        auto txindex_num = butil::Hash(iter->first) % _txindexs.size();
        azino::txindex::TxOpService_Stub stub(_txindexs[txindex_num].get());

        brpc::Controller cntl;
        azino::txindex::CommitRequest req;
        // todo: avoid memory copy here
        req.set_allocated_txid(new TxIdentifier(*_txid));
        req.set_key(iter->first);
        azino::txindex::CommitResponse resp;
        stub.Commit(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            LOG_CONTROLLER_ERROR(cntl, ss)
            return Status::NetworkErr(ss.str());
        }

        LOG_SDK(cntl, req, resp, Commit_from_txindex)

        switch (resp.tx_op_status().error_code()) {
            case TxOpStatus_Code_Ok:
                iter->second.status = TxWriteStatus::COMMITTED;
                break;
            default:
                ss << " Commit key: " << iter->first
                   << " value: " << iter->second.value.ShortDebugString()
                   << " error code: " << resp.tx_op_status().error_code()
                   << " error message: " << resp.tx_op_status().error_message();
                return Status::TxIndexErr(ss.str());
        }
    }
    return Status::Ok();
}

Status Transaction::AbortAll() {
    std::stringstream ss;

    // todo: parallelize aborting
    for (auto iter = _txwritebuffer->begin(); iter != _txwritebuffer->end();
         iter++) {
        if (iter->second.status == TxWriteStatus::NONE) {
            continue;
        }

        // todo: use a wrapper
        auto txindex_num = butil::Hash(iter->first) % _txindexs.size();
        azino::txindex::TxOpService_Stub stub(_txindexs[txindex_num].get());

        brpc::Controller cntl;
        azino::txindex::CleanRequest req;
        // todo: avoid memory copy here
        req.set_allocated_txid(new TxIdentifier(*_txid));
        req.set_key(iter->first);
        azino::txindex::CleanResponse resp;
        stub.Clean(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            LOG_CONTROLLER_ERROR(cntl, ss)
            return Status::NetworkErr(ss.str());
        }

        LOG_SDK(cntl, req, resp, Clean_from_txindex)

        switch (resp.tx_op_status().error_code()) {
            case TxOpStatus_Code_Ok:
                iter->second.status = TxWriteStatus::NONE;
                break;
            default:
                ss << " Abort key: " << iter->first
                   << " value: " << iter->second.value.ShortDebugString()
                   << " error code: " << resp.tx_op_status().error_code()
                   << " error message: " << resp.tx_op_status().error_message();
                return Status::TxIndexErr(ss.str());
        }
    }
    return Status::Ok();
}

Status Transaction::Put(const WriteOptions& options, const UserKey& key,
                        const UserValue& value) {
    return Write(options, key, false, value);
}

Status Transaction::Delete(const WriteOptions& options, const UserKey& key) {
    return Write(options, key, true);
}

Status Transaction::Write(const WriteOptions& options, const UserKey& key,
                          bool is_delete, const UserValue& value) {
    std::stringstream ss;
    if (!_txid) {
        ss << " Transaction has not began.";
        return Status::IllegalTxOp(ss.str());
    }
    if (_txid->status().status_code() != TxStatus_Code_Start) {
        ss << " Transaction is not allowed to put. "
           << _txid->ShortDebugString();
        return Status::IllegalTxOp(ss.str());
    }

    auto iter = _txwritebuffer->find(key);
    if (options.type == kPessimistic &&
        (iter == _txwritebuffer->end() ||
         iter->second.status != TxWriteStatus::LOCKED)) {
        // Pessimistic

        // todo: use a wrapper
        auto txindex_num = butil::Hash(key) % _txindexs.size();
        azino::txindex::TxOpService_Stub stub(_txindexs[txindex_num].get());

        brpc::Controller cntl;
        azino::txindex::WriteLockRequest req;
        // todo: avoid memory copy here
        req.set_key(key);
        req.set_allocated_txid(new TxIdentifier(*_txid));
        azino::txindex::WriteLockResponse resp;
        stub.WriteLock(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            LOG_CONTROLLER_ERROR(cntl, ss)
            return Status::NetworkErr(ss.str());
        }

        LOG_SDK(cntl, req, resp, WriteLock_from_txindex)

        switch (resp.tx_op_status().error_code()) {
            case TxOpStatus_Code_Ok:
                _txwritebuffer->Upsert(options, key, is_delete, value);
                iter = _txwritebuffer->find(key);
                iter->second.status = TxWriteStatus::LOCKED;
                break;
            default:
                // todo: fail to lock, may use some optimistic approach
                ss << " Lock key: " << key << " value: " << value
                   << " is_delete: " << is_delete
                   << " error code: " << resp.tx_op_status().error_code()
                   << " error message: " << resp.tx_op_status().error_message();
                return Status::TxIndexErr(ss.str());
        }
    } else {
        _txwritebuffer->Upsert(options, key, is_delete, value);
    }

    return Status::Ok();
}

Status Transaction::Get(const ReadOptions& options, const UserKey& key,
                        UserValue& value) {
    std::stringstream ss;

    auto iter = _txwritebuffer->find(key);
    if (iter != _txwritebuffer->end()) {
        auto v = iter->second.value;
        ss << " Find in TxWriteBuffer Key: " << key
           << " Value: " << v.ShortDebugString();
        if (v.is_delete()) {
            return Status::NotFound(ss.str());
        } else {
            value = v.content();
            return Status::Ok(ss.str());
        }
    }

    // todo: use a wrapper
    auto txindex_num = butil::Hash(key) % _txindexs.size();
    azino::txindex::TxOpService_Stub stub(_txindexs[txindex_num].get());

    brpc::Controller cntl;
    azino::txindex::ReadRequest req;
    // todo: avoid memory copy here
    req.set_key(key);
    req.set_allocated_txid(new TxIdentifier(*_txid));
    azino::txindex::ReadResponse resp;
    stub.Read(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        LOG_CONTROLLER_ERROR(cntl, ss)
        return Status::NetworkErr(ss.str());
    }

    LOG_SDK(cntl, req, resp, Read_from_txindex)

    switch (resp.tx_op_status().error_code()) {
        case TxOpStatus_Code_Ok:
            ss << " Find in TxIndex Key: " << key
               << " Value: " << resp.value().ShortDebugString();
            if (resp.value().is_delete()) {
                return Status::NotFound(ss.str());
            } else {
                value = resp.value().content();
                return Status::Ok(ss.str());
            }
        default:
            ss << " Find in TxIndex Key: " << key
               << " value: " << resp.value().ShortDebugString()
               << " error code: " << resp.tx_op_status().error_code()
               << " error message: " << resp.tx_op_status().error_message();
            if (resp.tx_op_status().error_code() ==
                TxOpStatus_Code_ReadNotExist) {
                goto readStorage;
            }
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
        LOG_CONTROLLER_ERROR(storage_cntl, storage_ss)
        return Status::NetworkErr(storage_ss.str());
    }

    LOG_SDK(storage_cntl, storage_req, storage_resp, Read_from_storage)

    switch (storage_resp.status().error_code()) {
        case storage::StorageStatus_Code_Ok:
            ss << " Find in Storage Key: " << key
               << " Value: " << storage_resp.value();
            value = storage_resp.value();
            return Status::Ok(storage_ss.str());
        case storage::StorageStatus_Code_NotFound:
            ss << " Find in Storage Key: " << key << " No Value";
            return Status::NotFound(storage_ss.str());
        default:
            ss << " Find in Storage Key: " << key
               << " value: " << storage_resp.value()
               << " error code: " << storage_resp.status().error_code()
               << " error message: " << storage_resp.status().error_message();
            return Status::StorageErr(storage_ss.str());
    }
}
}  // namespace azino