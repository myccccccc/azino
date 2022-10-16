
#include "txidtable.h"

namespace azino {
namespace txplanner {

class TxDependence : public Dependence {
   public:
    TxDependence(DepType type, TimeStamp target_tx_start_ts)
        : _type(type), _target_tx_start_ts(target_tx_start_ts) {}
    virtual ~TxDependence() = default;
    virtual DepType Type() const override { return _type; }
    virtual uint64_t ID() const override { return _target_tx_start_ts; }

   private:
    DepType _type;
    TimeStamp _target_tx_start_ts;
};

class TxID {
   public:
    TxID() : _txid() {}
    void reset_txid(const TxIdentifier& txid) { _txid = txid; }

    TimeStamp start_ts() { return _txid.start_ts(); }

    void add_in_dep(DepType type, TimeStamp income_start_ts) {
        DependencePtr p(new TxDependence(type, income_start_ts));
        _in.insert(p);
    }

    DependenceSet get_in_dep() { return _in; }

    DependenceSet get_out_dep() { return _out; }

    void add_out_dep(DepType type, TimeStamp outcome_start_ts) {
        DependencePtr p(new TxDependence(type, outcome_start_ts));
        _out.insert(p);
    }

   private:
    TxIdentifier _txid;
    DependenceSet _in;
    DependenceSet _out;
};

DependenceSet TxIDTable::GetInDependence(TimeStamp ts) {
    std::lock_guard<bthread::Mutex> lck(_m);

    if (_table.find(ts) != _table.end()) {
        return _table[ts]->get_in_dep();
    } else {
        LOG(WARNING) << "Fail to find ts: " << ts;
        return {};
    }
}

DependenceSet TxIDTable::GetOutDependence(TimeStamp ts) {
    std::lock_guard<bthread::Mutex> lck(_m);

    if (_table.find(ts) != _table.end()) {
        return _table[ts]->get_out_dep();
    } else {
        LOG(WARNING) << "Fail to find ts: " << ts;
        return {};
    }
}

IDSet TxIDTable::ListID() {
    std::lock_guard<bthread::Mutex> lck(_m);

    IDSet res;
    for (auto iter = _table.begin(); iter != _table.end(); iter++) {
        res.insert(iter->second->start_ts());
    }
    return res;
}

void TxIDTable::UpsertTxID(const TxIdentifier& txid, TimeStamp start_ts,
                           TimeStamp commit_ts) {
    std::lock_guard<bthread::Mutex> lck(_m);

    if (_table.find(start_ts) == _table.end()) {
        _table[start_ts].reset(new TxID());
    }

    _table[start_ts]->reset_txid(txid);

    if (commit_ts != MIN_TIMESTAMP) {
        _table[commit_ts] = _table[start_ts];
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
    if (_table.find(ts2) == _table.end()) {
        LOG(ERROR) << "Fail to add dependency type: " << type << "ts1: " << ts1
                   << "ts2: " << ts2 << "(not found) ";
        return ENOENT;
    }

    _table[ts1]->add_out_dep(type, _table[ts2]->start_ts());
    _table[ts2]->add_in_dep(type, _table[ts1]->start_ts());
    return 0;
}
}  // namespace txplanner
}  // namespace azino