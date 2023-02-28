#include "azino/client.h"

#include <brpc/channel.h>
#include <butil/hash.h>

#include "azino/partition.h"
#include "service/storage/storage.pb.h"
#include "service/tx.pb.h"
#include "service/txindex/txindex.pb.h"
#include "service/txplanner/txplanner.pb.h"
#include "txwritebuffer.h"

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

#define BEGIN_CHECK(action)                                     \
    if (!_txid) {                                               \
        ss << " Transaction has not began.";                    \
        return Status::IllegalTxOp(ss.str());                   \
    }                                                           \
    if (_txid->status().status_code() != TxStatus_Code_Start) { \
        ss << " Transaction is not allowed to " << #action      \
           << _txid->ShortDebugString();                        \
        return Status::IllegalTxOp(ss.str());                   \
    }

DEFINE_int32(timeout_ms, -1, "RPC timeout in milliseconds");

static brpc::ChannelOptions channel_options;

namespace azino {
Transaction::Transaction(const Options& options,
                         const std::string& txplanner_addr)
    : _options(options), _txid(nullptr), _txwritebuffer(nullptr) {
    channel_options.timeout_ms = FLAGS_timeout_ms;

    std::stringstream ss;
    auto* channel = new brpc::Channel();
    int err;
    err = channel->Init(txplanner_addr.c_str(), &channel_options);
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
    brpc::Controller cntl;
    azino::txplanner::BeginTxRequest req;
    azino::txplanner::BeginTxResponse resp;
    azino::txplanner::TxService_Stub stub(_txplanner.get());
    if (_txid) {
        ss << " Transaction has already began. " << _txid->ShortDebugString();
        return Status::IllegalTxOp(ss.str());
    }

    stub.BeginTx(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        LOG_CONTROLLER_ERROR(cntl, ss)
        return Status::NetworkErr(ss.str());
    }

    //    LOG_SDK(cntl, req, resp, BeginTx_from_txplanner)

    _txid.reset(resp.release_txid());
    if (_txid->status().status_code() != TxStatus_Code_Start) {
        ss << " Wrong tx status code: " << _txid->status().status_code();
        return Status::TxPlannerErr(ss.str());
    }
    _txwritebuffer.reset(new TxWriteBuffer);
    auto partition = Partition::FromPB(resp.partition());

    // init storage channel
    auto storage_addr = partition.GetStorage();
    if (_channel_table.find(storage_addr) == _channel_table.end()) {
        ChannelPtr channel(new brpc::Channel());
        err = channel->Init(storage_addr.c_str(), &channel_options);
        if (err) {
            LOG_CHANNEL_ERROR(storage_addr.c_str(), err, ss)
            return Status::NetworkErr(ss.str());
        }
        _channel_table.insert(std::make_pair(storage_addr, channel));
    }
    _storage = _channel_table.find(storage_addr)->second;

    // init txindex channels
    for (auto iter = partition.GetPartitionConfigMap().begin();
         iter != partition.GetPartitionConfigMap().end(); iter++) {
        auto& range = iter->first;
        auto& partition_config = iter->second;
        auto txindex_addr = partition_config.GetTxIndex();
        if (_channel_table.find(txindex_addr) == _channel_table.end()) {
            ChannelPtr channel(new brpc::Channel());
            err = channel->Init(txindex_addr.c_str(), &channel_options);
            if (err) {
                LOG_CHANNEL_ERROR(txindex_addr, err, ss)
                return Status::NetworkErr(ss.str());
            }
            _channel_table.insert(std::make_pair(txindex_addr, channel));
        }
        auto channel = _channel_table.find(txindex_addr)->second;
        _route_table.insert(std::make_pair(
            range, Region{channel, partition_config.GetPessimismKey()}));
    }

    return Status::Ok();
}

Status Transaction::Abort(Status reason) {
    std::stringstream ss;
    brpc::Controller cntl;
    azino::txplanner::AbortTxRequest areq;
    azino::txplanner::AbortTxResponse aresp;
    azino::txplanner::TxService_Stub stub(_txplanner.get());
    BEGIN_CHECK(abort)

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

    if (_txid->status().status_code() != TxStatus_Code_Abort) {
        ss << " Wrong tx status code when abort: " << _txid->ShortDebugString();
        return Status::TxPlannerErr(ss.str());
    }

    auto abort_sts = AbortAll();
    if (abort_sts.IsOk()) {
        _txid->mutable_status()->set_status_message(reason.ToString());
        return reason;
    } else {
        _txid->mutable_status()->set_status_code(TxStatus_Code_Abnormal);
        _txid->mutable_status()->set_status_message(abort_sts.ToString());
        return abort_sts;
    }
}

Status Transaction::Commit() {
    std::stringstream ss;
    azino::txplanner::TxService_Stub stub(_txplanner.get());
    brpc::Controller cntl;
    azino::txplanner::CommitTxRequest req;
    azino::txplanner::CommitTxResponse resp;
    BEGIN_CHECK(commit)

    _txid->mutable_status()->set_status_code(TxStatus_Code_Preput);
    auto preput_sts = PreputAll();
    if (!preput_sts.IsOk()) {
        return Abort(preput_sts);
    }

    req.set_allocated_txid(new TxIdentifier(*_txid));
    stub.CommitTx(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        LOG_CONTROLLER_ERROR(cntl, ss)
        return Status::NetworkErr(ss.str());
    }

    LOG_SDK(cntl, req, resp, CommitTx_from_txplanner)

    _txid.reset(resp.release_txid());
    if (_txid->status().status_code() != TxStatus_Code_Commit) {
        ss << " Wrong tx status code when commit: "
           << _txid->ShortDebugString();
        return Abort(Status::TxPlannerErr(ss.str()));
    }

    auto commit_sts = CommitAll();
    if (commit_sts.IsOk()) {
        _txid->mutable_status()->set_status_message(commit_sts.ToString());
        return commit_sts;
    } else {
        _txid->mutable_status()->set_status_code(TxStatus_Code_Abnormal);
        _txid->mutable_status()->set_status_message(commit_sts.ToString());
        return commit_sts;
    }
}

Status Transaction::PreputAll() {
    std::stringstream ss;

    // todo: parallelize preputing
    for (auto iter = _txwritebuffer->begin(); iter != _txwritebuffer->end();
         iter++) {
        assert(iter->second.status < TxWriteStatus::PREPUTED);

        azino::txindex::TxOpService_Stub stub(Route(iter->first).channel.get());

        brpc::Controller cntl;
        azino::txindex::WriteIntentRequest req;
        azino::txindex::WriteIntentResponse resp;
        req.set_allocated_txid(new TxIdentifier(*_txid));
        req.set_key(iter->first);
        req.set_allocated_value(new Value(iter->second.value));
        stub.WriteIntent(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            LOG_CONTROLLER_ERROR(cntl, ss)
            return Status::NetworkErr(ss.str());
        }

        LOG_SDK(cntl, req, resp, WriteIntent_from_txindex)

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

        azino::txindex::TxOpService_Stub stub(Route(iter->first).channel.get());

        brpc::Controller cntl;
        azino::txindex::CommitRequest req;
        azino::txindex::CommitResponse resp;
        req.set_allocated_txid(new TxIdentifier(*_txid));
        req.set_key(iter->first);
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

        azino::txindex::TxOpService_Stub stub(Route(iter->first).channel.get());

        brpc::Controller cntl;
        azino::txindex::CleanRequest req;
        azino::txindex::CleanResponse resp;
        req.set_allocated_txid(new TxIdentifier(*_txid));
        req.set_key(iter->first);
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

Status Transaction::Put(WriteOptions options, const UserKey& key,
                        const UserValue& value) {
    return Write(options, key, false, value);
}

Status Transaction::Delete(WriteOptions options, const UserKey& key) {
    return Write(options, key, true);
}

Status Transaction::Write(WriteOptions options, const UserKey& key,
                          bool is_delete, const UserValue& value) {
    std::stringstream ss;
    BEGIN_CHECK(write);
    auto& region = Route(key);

    auto iter = _txwritebuffer->find(key);
    if (options.type == kAutomatic && region.pk.find(key) != region.pk.end()) {
        options.type = kPessimistic;
    }
    if (options.type == kPessimistic &&
        (iter == _txwritebuffer->end() ||
         iter->second.status < TxWriteStatus::LOCKED)) {
        // Pessimistic

        azino::txindex::TxOpService_Stub stub(region.channel.get());

        brpc::Controller cntl;
        azino::txindex::WriteLockRequest req;
        azino::txindex::WriteLockResponse resp;
        req.set_key(key);
        req.set_allocated_txid(new TxIdentifier(*_txid));
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

Status Transaction::Get(ReadOptions options, const UserKey& key,
                        UserValue& value) {
    std::stringstream ss;
    BEGIN_CHECK(read)

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

    azino::txindex::TxOpService_Stub stub(Route(key).channel.get());

    brpc::Controller cntl;
    azino::txindex::ReadRequest req;
    azino::txindex::ReadResponse resp;
    req.set_key(key);
    req.set_allocated_txid(new TxIdentifier(*_txid));
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
        case TxOpStatus_Code_NotExist:
            goto readStorage;
        default:
            ss << " Find in TxIndex Key: " << key
               << " value: " << resp.value().ShortDebugString()
               << " error code: " << resp.tx_op_status().error_code()
               << " error message: " << resp.tx_op_status().error_message();
            return Status::TxIndexErr(ss.str());
    }

readStorage:
    azino::storage::StorageService_Stub storage_stub(_storage.get());
    brpc::Controller storage_cntl;
    azino::storage::MVCCGetRequest storage_req;
    azino::storage::MVCCGetResponse storage_resp;
    storage_req.set_key(key);
    storage_req.set_ts(_txid->start_ts());
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

Region& Transaction::Route(const std::string& key) {
    auto key_range = azino::Range(key, key, 1, 1);
    auto iter = _route_table.lower_bound(key_range);
    if (iter == _route_table.end() || !iter->first.Contains(key_range)) {
        LOG(FATAL) << "Fail to route key:" << key;
    }
    return iter->second;
}

Status Transaction::Scan(const UserKey& left_key, const UserKey& right_key,
                         std::vector<UserValue>& keys,
                         std::vector<UserValue>& values) {
    std::stringstream ss;
    BEGIN_CHECK(scan)

    azino::storage::StorageService_Stub storage_stub(_storage.get());
    brpc::Controller storage_cntl;
    azino::storage::MVCCScanRequest storage_req;
    azino::storage::MVCCScanResponse storage_resp;
    storage_req.set_left_key(left_key);
    storage_req.set_right_key(right_key);
    storage_req.set_ts(_txid->start_ts());
    storage_stub.MVCCScan(&storage_cntl, &storage_req, &storage_resp, nullptr);
    std::stringstream storage_ss;
    if (storage_cntl.Failed()) {
        LOG_CONTROLLER_ERROR(storage_cntl, storage_ss)
        return Status::NetworkErr(storage_ss.str());
    }

    LOG_SDK(storage_cntl, storage_req, storage_resp, Scan_from_storage)

    // TODO: merge with data in _txwritebuffer
    switch (storage_resp.status().error_code()) {
        case storage::StorageStatus_Code_Ok:
            ss << " Find in Storage LeftKey: " << left_key
               << " RightKey: " << right_key;
            for (int i = 0; i < storage_resp.value_size(); i++) {
                keys.push_back(storage_resp.key(i));
                values.push_back(storage_resp.value(i));
            }
            return Status::Ok(storage_ss.str());
        case storage::StorageStatus_Code_NotFound:
            ss << " Find in Storage LeftKey: " << left_key
               << " RightKey: " << right_key << " No Value";
            return Status::NotFound(storage_ss.str());
        default:
            ss << " Find in Storage LeftKey: " << left_key
               << " RightKey: " << right_key
               << " error code: " << storage_resp.status().error_code()
               << " error message: " << storage_resp.status().error_message();
            return Status::StorageErr(storage_ss.str());
    }
}

void Transaction::Reset() {
    _txid.reset();
    _txwritebuffer.reset();
    _route_table.clear();
}
}  // namespace azino