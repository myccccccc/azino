#include "index.h"

namespace azino {
namespace txindex {
TxIndex::TxIndex(brpc::Channel *storage_channel,
                 brpc::Channel *txplaner_channel)
    : _storage_channel(storage_channel),
      _txplaner_channel(txplaner_channel),
      _region(new KVRegion(storage_channel, txplaner_channel)) {}

TxOpStatus TxIndex::WriteLock(const std::string &key, const TxIdentifier &txid,
                              std::function<void()> callback) {
    return _region->WriteLock(key, txid, callback);
}

TxOpStatus TxIndex::WriteIntent(const std::string &key, const Value &value,
                                const TxIdentifier &txid,
                                std::function<void()> callback) {
    return _region->WriteIntent(key, value, txid, callback);
}

TxOpStatus TxIndex::Clean(const std::string &key, const TxIdentifier &txid) {
    return _region->Clean(key, txid);
}

TxOpStatus TxIndex::Commit(const std::string &key, const TxIdentifier &txid) {
    return _region->Commit(key, txid);
}

TxOpStatus TxIndex::Read(const std::string &key, Value &v,
                         const TxIdentifier &txid,
                         std::function<void()> callback) {
    return _region->Read(key, v, txid, callback);
}
}  // namespace txindex
}  // namespace azino