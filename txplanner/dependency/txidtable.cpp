#include "txidtable.h"

namespace azino {
namespace txplanner {

TxIDPtrSet TxIDTable::GetInDependence(uint64_t ts) {
    std::lock_guard<bthread::Mutex> lck(_m);

    if (_table.find(ts) != _table.end()) {
        return _table[ts]->in;
    } else {
        LOG(WARNING) << "Fail to find ts: " << ts;
        return {};
    }
}

TxIDPtrSet TxIDTable::GetOutDependence(uint64_t ts) {
    std::lock_guard<bthread::Mutex> lck(_m);

    if (_table.find(ts) != _table.end()) {
        return _table[ts]->out;
    } else {
        LOG(WARNING) << "Fail to find ts: " << ts;
        return {};
    }
}

TxIDPtrSet TxIDTable::List() {
    std::lock_guard<bthread::Mutex> lck(_m);

    TxIDPtrSet res;
    for (auto& iter : _table) {
        res.insert(iter.second);
    }
    return res;
}

void TxIDTable::UpsertTxID(const TxIdentifier& txid) {
    std::lock_guard<bthread::Mutex> lck(_m);

    if (_table.find(txid.start_ts()) == _table.end()) {
        _table[txid.start_ts()].reset(new TxID());
    }

    _table[txid.start_ts()]->reset_txid(txid);

    if (txid.has_commit_ts()) {
        _table[txid.commit_ts()] = _table[txid.start_ts()];
    }
}

int TxIDTable::AddDep(DepType type, TimeStamp ts1, TimeStamp ts2) {
    std::lock_guard<bthread::Mutex> lck(_m);

    if (_table.find(ts1) == _table.end()) {
        LOG(ERROR) << "Fail to add dependency type: " << type << "ts1: " << ts1
                   << "(not found) "
                   << "ts2: " << ts2;
        return ENOENT;
    }
    auto p1 = _table[ts1];

    if (_table.find(ts2) == _table.end()) {
        LOG(ERROR) << "Fail to add dependency type: " << type << "ts1: " << ts1
                   << "ts2: " << ts2 << "(not found) ";
        return ENOENT;
    }
    auto p2 = _table[ts2];

    _table[ts1]->out.insert(p2);
    _table[ts2]->in.insert(p1);
    return 0;
}

int TxIDTable::DeleteTxID(const TxIdentifier& txid) {
    std::lock_guard<bthread::Mutex> lck(_m);

    if (_table.find(txid.start_ts()) == _table.end()) {
        return ENOENT;
    }

    auto tx = _table[txid.start_ts()];
    for (const auto& dp : tx->in) {
        dp->out.erase(tx);
    }
    for (const auto& dp : tx->out) {
        dp->in.erase(tx);
    }

    _table.erase(txid.start_ts());
    if (txid.has_commit_ts()) {
        _table.erase(txid.commit_ts());
    }

    return 0;
}
}  // namespace txplanner
}  // namespace azino