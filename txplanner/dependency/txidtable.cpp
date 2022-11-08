
#include "txidtable.h"

namespace azino {
namespace txplanner {

class TxDependence : public Dependence {
   public:
    TxDependence(DepType type, TimeStamp from_tx_start_ts,
                 TimeStamp to_tx_start_ts)
        : _type(type),
          _from_tx_start_ts(from_tx_start_ts),
          _to_tx_start_ts(to_tx_start_ts) {}
    virtual ~TxDependence() = default;
    virtual DepType Type() const override { return _type; }
    virtual uint64_t fromID() const override { return _from_tx_start_ts; }
    virtual uint64_t toID() const override { return _to_tx_start_ts; }

   private:
    DepType _type;
    TimeStamp _from_tx_start_ts;
    TimeStamp _to_tx_start_ts;
};

class TxID {
   public:
    TxID() : _txid() {}
    void reset_txid(const TxIdentifier& txid) { _txid = txid; }

    TimeStamp start_ts() { return _txid.start_ts(); }

    void add_in_dep(const DependencePtr& p) {
        assert(p->toID() == _txid.start_ts());
        _in.insert(p);
    }

    DependenceSet get_in_dep() { return _in; }

    DependenceSet get_out_dep() { return _out; }

    void add_out_dep(const DependencePtr& p) {
        assert(p->fromID() == _txid.start_ts());
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

    DependencePtr p(new TxDependence(type, _table[ts1]->start_ts(),
                                     _table[ts2]->start_ts()));

    _table[ts1]->add_out_dep(p);
    _table[ts2]->add_in_dep(p);
    return 0;
}
}  // namespace txplanner
}  // namespace azino