#include "azino/client.h"

#include <brpc/channel.h>
#include <butil/hash.h>

#include "azino/partition.h"
#include "service/storage/storage.pb.h"
#include "service/tx.pb.h"
#include "service/txindex/txindex.pb.h"
#include "service/txplanner/txplanner.pb.h"
#include "txrwbuffer.h"

#define LOG_WRONG_TX_STATUS_CODE(ss, op)              \
    ss << " Wrong tx status code when " << #op << ":" \
       << _txid->ShortDebugString();                  \
    LOG(ERROR) << ss.str();

#define LOG_CHANNEL_ERROR(addr, err, ss)                                     \
    ss << " Fail to initialize channel: " << addr << " error code: " << err; \
    LOG(ERROR) << ss.str();

#define LOG_CONTROLLER_ERROR(cntl, ss)                                      \
    LOG(ERROR) << " Sdk controller failed error code: " << cntl.ErrorCode() \
               << " error text: " << cntl.ErrorText();

#define LOG_SDK(cntl, req, resp, msg)                                         \
    LOG(INFO) << " Sdk: " << cntl.local_side() << " " << #msg << ": "         \
              << cntl.remote_side() << " request: " << req.ShortDebugString() \
              << " response: " << resp.ShortDebugString()                     \
              << " latency= " << cntl.latency_us() << "us" << std::endl;

#define BEGIN_CHECK(action)                                        \
    if (!_txid) {                                                  \
        return Status::IllegalTxOp(" Transaction has not began."); \
    }                                                              \
    if (_txid->status().status_code() < TxStatus_Code_Start) {     \
        std::stringstream ss;                                      \
        ss << " Transaction is not allowed to " << #action         \
           << _txid->ShortDebugString();                           \
        return Status::IllegalTxOp(ss.str());                      \
    }

DEFINE_int32(timeout_ms, -1, "RPC timeout in milliseconds");

static brpc::ChannelOptions channel_options;

namespace azino {
Transaction::Transaction(const Options& options)
    : _options(options), _txid(nullptr), _txrwbuffer(nullptr) {
    channel_options.timeout_ms = FLAGS_timeout_ms;

    auto* channel = new brpc::Channel();
    int err;
    err = channel->Init(options.txplanner_addr.c_str(), &channel_options);
    if (err) {
        std::stringstream ss;
        LOG_CHANNEL_ERROR(options.txplanner_addr, err, ss)
        return;
    }
    _txplanner.reset(channel);
}

Transaction::~Transaction() = default;

Status Transaction::Begin() {
    int err = 0;
    brpc::Controller cntl;
    azino::txplanner::BeginTxRequest req;
    azino::txplanner::BeginTxResponse resp;
    azino::txplanner::TxService_Stub stub(_txplanner.get());
    if (_txid) {
        std::stringstream ss;
        ss << " Transaction has already began. " << _txid->ShortDebugString();
        return Status::IllegalTxOp(ss.str());
    }

    stub.BeginTx(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        std::stringstream ss;
        LOG_CONTROLLER_ERROR(cntl, ss)
        return Status::NetworkErr(ss.str());
    }

    LOG_SDK(cntl, req, resp, BeginTx_from_txplanner)

    _txid.reset(resp.release_txid());
    if (_txid->status().status_code() != TxStatus_Code_Start) {
        std::stringstream ss;
        LOG_WRONG_TX_STATUS_CODE(ss, begin)
        return Status::TxPlannerErr(ss.str());
    }
    _txrwbuffer.reset(new TxRWBuffer);
    auto partition = Partition::FromPB(resp.partition());

    // init storage channel
    auto storage_addr = partition.GetStorage();
    if (_channel_table.find(storage_addr) == _channel_table.end()) {
        ChannelPtr channel(new brpc::Channel());
        err = channel->Init(storage_addr.c_str(), &channel_options);
        if (err) {
            std::stringstream ss;
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
                std::stringstream ss;
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
    brpc::Controller cntl;
    azino::txplanner::AbortTxRequest areq;
    azino::txplanner::AbortTxResponse aresp;
    azino::txplanner::TxService_Stub stub(_txplanner.get());
    BEGIN_CHECK(abort)

    if (_txid->status().status_code() != TxStatus_Code_Abort) {
        areq.set_allocated_txid(new TxIdentifier(*_txid));
        stub.AbortTx(&cntl, &areq, &aresp, nullptr);
        if (cntl.Failed()) {
            std::stringstream ss;
            LOG_CONTROLLER_ERROR(cntl, ss)
            return Status::NetworkErr(ss.str());
        }

        LOG_SDK(cntl, areq, aresp, AbortTx_from_txplanner)

        _txid.reset(aresp.release_txid());
    }

    if (_txid->status().status_code() != TxStatus_Code_Abort) {
        std::stringstream ss;
        LOG_WRONG_TX_STATUS_CODE(ss, abort)
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
        std::stringstream ss;
        LOG_CONTROLLER_ERROR(cntl, ss)
        return Status::NetworkErr(ss.str());
    }

    LOG_SDK(cntl, req, resp, CommitTx_from_txplanner)

    _txid.reset(resp.release_txid());
    if (_txid->status().status_code() != TxStatus_Code_Commit) {
        std::stringstream ss;
        LOG_WRONG_TX_STATUS_CODE(ss, commit)
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

Status Transaction::preput(const std::string& key, TxRW& tx_rw) {
    azino::txindex::TxOpService_Stub stub(Route(key).channel.get());
    brpc::Controller cntl;
    azino::txindex::WriteIntentRequest req;
    azino::txindex::WriteIntentResponse resp;

    req.set_allocated_txid(new TxIdentifier(*_txid));
    req.set_key(key);
    req.set_allocated_value(new Value(tx_rw.Value()));
    stub.WriteIntent(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        std::stringstream ss;
        LOG_CONTROLLER_ERROR(cntl, ss)
        return Status::NetworkErr(ss.str());
    }

    LOG_SDK(cntl, req, resp, WriteIntent_from_txindex)

    switch (resp.tx_op_status().error_code()) {
        case TxOpStatus_Code_Ok:
            tx_rw.Status() = PREPUTED;
            break;
        default:
            std::stringstream ss;
            ss << " Preput key: " << key
               << " error code: " << resp.tx_op_status().error_code()
               << " error message: " << resp.tx_op_status().error_message();
            return Status::TxIndexErr(ss.str());
    }
}

Status Transaction::PreputAll() {
    auto sts = Status::Ok();

    // todo: parallelize preputing
    for (auto iter = _txrwbuffer->begin(); iter != _txrwbuffer->end(); iter++) {
        std::string value;
        auto& key = iter->first;
        auto& tx_rw = iter->second;

        switch (tx_rw.Type()) {
            case SERIALIZABLE_READ:
                sts = read(key, value, tx_rw, true);
                break;
            case WRITE:
                sts = preput(key, tx_rw);
                break;
            case READ:
            default:
                continue;
        }

        if (!sts.IsOk()) {
            return sts;
        }
    }
    return Status::Ok();
}

Status Transaction::commit(const std::string& key, TxRW& tx_rw) {
    azino::txindex::TxOpService_Stub stub(Route(key).channel.get());

    brpc::Controller cntl;
    azino::txindex::CommitRequest req;
    azino::txindex::CommitResponse resp;
    req.set_allocated_txid(new TxIdentifier(*_txid));
    req.set_key(key);
    stub.Commit(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        std::stringstream ss;
        LOG_CONTROLLER_ERROR(cntl, ss)
        return Status::NetworkErr(ss.str());
    }

    LOG_SDK(cntl, req, resp, Commit_from_txindex)

    switch (resp.tx_op_status().error_code()) {
        case TxOpStatus_Code_Ok:
            tx_rw.Status() = COMMITTED;
            break;
        default:
            std::stringstream ss;
            ss << " Commit key: " << key
               << " error code: " << resp.tx_op_status().error_code()
               << " error message: " << resp.tx_op_status().error_message();
            return Status::TxIndexErr(ss.str());
    }
}

Status Transaction::CommitAll() {
    auto sts = Status::Ok();

    // todo: parallelize committing
    for (auto iter = _txrwbuffer->begin(); iter != _txrwbuffer->end(); iter++) {
        auto& key = iter->first;
        auto& tx_rw = iter->second;

        switch (tx_rw.Type()) {
            case SERIALIZABLE_READ:
                sts = clean(key, tx_rw);
                break;
            case WRITE:
                sts = commit(key, tx_rw);
                break;
            case READ:
            default:
                continue;
        }

        if (!sts.IsOk()) {
            return sts;
        }
    }
    return Status::Ok();
}

Status Transaction::clean(const std::string& key, TxRW& tx_rw) {
    azino::txindex::TxOpService_Stub stub(Route(key).channel.get());

    brpc::Controller cntl;
    azino::txindex::CleanRequest req;
    azino::txindex::CleanResponse resp;
    req.set_allocated_txid(new TxIdentifier(*_txid));
    req.set_key(key);
    stub.Clean(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        std::stringstream ss;
        LOG_CONTROLLER_ERROR(cntl, ss)
        return Status::NetworkErr(ss.str());
    }

    LOG_SDK(cntl, req, resp, Clean_from_txindex)

    switch (resp.tx_op_status().error_code()) {
        case TxOpStatus_Code_Ok:
            tx_rw.Status() = NONE;
            break;
        default:
            std::stringstream ss;
            ss << " Abort key: " << key
               << " error code: " << resp.tx_op_status().error_code()
               << " error message: " << resp.tx_op_status().error_message();
            return Status::TxIndexErr(ss.str());
    }
}

Status Transaction::AbortAll() {
    auto sts = Status::Ok();

    // todo: parallelize aborting
    for (auto iter = _txrwbuffer->begin(); iter != _txrwbuffer->end(); iter++) {
        auto& key = iter->first;
        auto& tx_rw = iter->second;

        if (tx_rw.Status() < LOCKED) {
            continue;
        }

        sts = clean(key, tx_rw);
        if (!sts.IsOk()) {
            return sts;
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

Status Transaction::write_lock(const std::string& key, TxRW& tx_rw) {
    auto& region = Route(key);
    azino::txindex::TxOpService_Stub stub(region.channel.get());
    brpc::Controller cntl;
    azino::txindex::WriteLockRequest req;
    azino::txindex::WriteLockResponse resp;
    req.set_key(key);
    req.set_allocated_txid(new TxIdentifier(*_txid));
    stub.WriteLock(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        std::stringstream ss;
        LOG_CONTROLLER_ERROR(cntl, ss)
        return Status::NetworkErr(ss.str());
    }

    LOG_SDK(cntl, req, resp, WriteLock_from_txindex)

    switch (resp.tx_op_status().error_code()) {
        case TxOpStatus_Code_Ok:
            tx_rw.Status() = LOCKED;
            break;
        default:
            std::stringstream ss;
            ss << " Lock key: " << key
               << " error code: " << resp.tx_op_status().error_code()
               << " error message: " << resp.tx_op_status().error_message();
            return Status::TxIndexErr(ss.str());
    }
}

Status Transaction::Write(WriteOptions options, const UserKey& key,
                          bool is_delete, const UserValue& value) {
    BEGIN_CHECK(write);
    auto& region = Route(key);
    auto sts = Status::Ok();

    if (options.type == kAutomatic && region.pk.find(key) != region.pk.end()) {
        options.type = kPessimistic;
    }
    auto tx_rw = TxRW(WRITE, NONE, is_delete, value);
    if (options.type == kPessimistic) {
        auto iter = _txrwbuffer->find(key);
        if (iter == _txrwbuffer->end() || iter->second.Status() < LOCKED) {
            sts = write_lock(key, tx_rw);
            if (!sts.IsOk()) {
                return sts;
            }
        }
    }
    _txrwbuffer->Upsert(key, tx_rw);

    return Status::Ok();
}

Status Transaction::Get(ReadOptions options, const UserKey& key,
                        UserValue& value) {
    BEGIN_CHECK(read)

    auto iter = _txwritebuffer->find(key);
    if (iter != _txwritebuffer->end()) {
        auto v = iter->second.value;
        if (v.is_delete()) {
            return Status::NotFound();
        } else {
            value = v.content();
            return Status::Ok();
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
        std::stringstream ss;
        LOG_CONTROLLER_ERROR(cntl, ss)
        return Status::NetworkErr(ss.str());
    }

    LOG_SDK(cntl, req, resp, Read_from_txindex)

    switch (resp.tx_op_status().error_code()) {
        case TxOpStatus_Code_Ok:
            if (resp.value().is_delete()) {
                return Status::NotFound();
            } else {
                value = resp.value().content();
                return Status::Ok();
            }
        case TxOpStatus_Code_NotExist:
            goto readStorage;
        default:
            std::stringstream ss;
            ss << " Find in TxIndex Key: "
               << key
               //               << " value: " << resp.value().ShortDebugString()
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
    if (storage_cntl.Failed()) {
        std::stringstream ss;
        LOG_CONTROLLER_ERROR(storage_cntl, ss)
        return Status::NetworkErr(ss.str());
    }

    LOG_SDK(storage_cntl, storage_req, storage_resp, Read_from_storage)

    switch (storage_resp.status().error_code()) {
        case storage::StorageStatus_Code_Ok:
            value = storage_resp.value();
            return Status::Ok();
        case storage::StorageStatus_Code_NotFound:
            return Status::NotFound();
        default:
            std::stringstream ss;
            ss << " Find in Storage Key: "
               << key
               //               << " value: " << storage_resp.value()
               << " error code: " << storage_resp.status().error_code()
               << " error message: " << storage_resp.status().error_message();
            return Status::StorageErr(ss.str());
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
    BEGIN_CHECK(scan)

    azino::storage::StorageService_Stub storage_stub(_storage.get());
    brpc::Controller storage_cntl;
    azino::storage::MVCCScanRequest storage_req;
    azino::storage::MVCCScanResponse storage_resp;
    storage_req.set_left_key(left_key);
    storage_req.set_right_key(right_key);
    storage_req.set_ts(_txid->start_ts());
    storage_stub.MVCCScan(&storage_cntl, &storage_req, &storage_resp, nullptr);
    if (storage_cntl.Failed()) {
        std::stringstream ss;
        LOG_CONTROLLER_ERROR(storage_cntl, storage_ss)
        return Status::NetworkErr(ss.str());
    }

    LOG_SDK(storage_cntl, storage_req, storage_resp, Scan_from_storage)

    // TODO: merge with data in _txwritebuffer
    switch (storage_resp.status().error_code()) {
        case storage::StorageStatus_Code_Ok:
            for (int i = 0; i < storage_resp.value_size(); i++) {
                keys.push_back(storage_resp.key(i));
                values.push_back(storage_resp.value(i));
            }
            return Status::Ok();
        case storage::StorageStatus_Code_NotFound:
            return Status::NotFound();
        default:
            std::stringstream ss;
            ss << " Find in Storage LeftKey: " << left_key
               << " RightKey: " << right_key
               << " error code: " << storage_resp.status().error_code()
               << " error message: " << storage_resp.status().error_message();
            return Status::StorageErr(ss.str());
    }
}

void Transaction::Reset() {
    _txid.reset();
    _txrwbuffer.reset();
    _route_table.clear();
}
}  // namespace azino