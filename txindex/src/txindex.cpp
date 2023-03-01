#include "index.h"

DEFINE_int32(storage_timeout_ms, 10000,
             "RPC timeout in milliseconds when access storage");

namespace azino {
namespace txindex {
TxIndex::TxIndex(brpc::Channel *txplaner_channel)
    : _txplaner_channel(txplaner_channel), _pm(txplaner_channel) {
    init_storage(_pm.GetPartition());
    init_region_table(_pm.GetPartition());
}

TxOpStatus TxIndex::WriteLock(const std::string &key, const TxIdentifier &txid,
                              std::function<void()> callback) {
    auto region = route(key);
    if (region == nullptr) {
        LOG(WARNING) << "Fail to route key:" << key;
        TxOpStatus sts;
        sts.set_error_code(TxOpStatus_Code_PartitionErr);
        return sts;
    }
    return region->WriteLock(key, txid, callback);
}

TxOpStatus TxIndex::WriteIntent(const std::string &key, const Value &value,
                                const TxIdentifier &txid,
                                std::function<void()> callback) {
    auto region = route(key);
    if (region == nullptr) {
        LOG(WARNING) << "Fail to route key:" << key;
        TxOpStatus sts;
        sts.set_error_code(TxOpStatus_Code_PartitionErr);
        return sts;
    }
    return region->WriteIntent(key, value, txid, callback);
}

TxOpStatus TxIndex::Clean(const std::string &key, const TxIdentifier &txid) {
    auto region = route(key);
    if (region == nullptr) {
        LOG(WARNING) << "Fail to route key:" << key;
        TxOpStatus sts;
        sts.set_error_code(TxOpStatus_Code_PartitionErr);
        return sts;
    }
    return region->Clean(key, txid);
}

TxOpStatus TxIndex::Commit(const std::string &key, const TxIdentifier &txid) {
    auto region = route(key);
    if (region == nullptr) {
        LOG(WARNING) << "Fail to route key:" << key;
        TxOpStatus sts;
        sts.set_error_code(TxOpStatus_Code_PartitionErr);
        return sts;
    }
    return region->Commit(key, txid);
}

TxOpStatus TxIndex::Read(const std::string &key, Value &v,
                         const TxIdentifier &txid,
                         std::function<void()> callback) {
    auto region = route(key);
    if (region == nullptr) {
        LOG(WARNING) << "Fail to route key:" << key;
        TxOpStatus sts;
        sts.set_error_code(TxOpStatus_Code_PartitionErr);
        return sts;
    }
    return region->Read(key, v, txid, callback);
}

KVRegionPtr TxIndex::route(const std::string &key) {
    auto key_range = Range(key, key, 1, 1);
    auto iter = _region_table.lower_bound(key_range);
    KVRegionPtr res;
    if (iter == _region_table.end() || !iter->first.Contains(key_range)) {
        res = nullptr;
    } else {
        res = iter->second;
    }
    return res;
}

void TxIndex::init_region_table(const Partition &p) {
    const auto &pcm = p.GetPartitionConfigMap();
    for (auto iter = pcm.begin(); iter != pcm.end(); iter++) {
        const auto &range = iter->first;
        const auto &pc = iter->second;
        if (pc.GetTxIndex() != FLAGS_txindex_addr) {
            continue;
        }
        _region_table.insert(std::make_pair(
            range, new KVRegion(range, &_storage_channel, _txplaner_channel)));
        LOG(INFO) << "TxIndex:" << FLAGS_txindex_addr
                  << " add partition:" << range.Describe();
    }
}

void TxIndex::init_storage(const Partition &p) {
    brpc::ChannelOptions options;
    options.timeout_ms = FLAGS_storage_timeout_ms;
    if (_storage_channel.Init(p.GetStorage().c_str(), &options) != 0) {
        LOG(FATAL) << "Fail to initialize storage channel";
    }
}

}  // namespace txindex
}  // namespace azino