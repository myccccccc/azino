
#include "txidtable.h"

namespace azino {
namespace txplanner {

class TxID : public Point {
   public:
    TxID() = default;
    virtual ~TxID() = default;
    void reset_txid(const TxIdentifier& txid) { _txid = txid; }

    virtual uint64_t ID() const override { return _txid.start_ts(); }
    virtual DependenceSet GetInDependence() const override { return _in; }
    virtual DependenceSet GetOutDependence() const override { return _out; }

   private:
    TxIdentifier _txid;
    DependenceSet _in;
    DependenceSet _out;

    friend class TxIDTable;

    void add_in_dependence(const DependencePtr& p) {
        assert(p->toPoint()->ID() == ID());
        _in.insert(p);
    }

    void add_out_dependence(const DependencePtr& p) {
        assert(p->fromPoint()->ID() == ID());
        _out.insert(p);
    }

    int del_in_dependence(const DependencePtr& p) {
        assert(p->toPoint()->ID() == ID());
        return _in.erase(p);
    }

    int del_out_dependence(const DependencePtr& p) {
        assert(p->fromPoint()->ID() == ID());
        return _out.erase(p);
    }
};

class TxDependence : public Dependence {
   public:
    TxDependence(DepType type, TxIDPtr from_tx, TxIDPtr to_tx)
        : _type(type), _from_tx(from_tx), _to_tx(to_tx) {}
    virtual ~TxDependence() = default;
    virtual DepType Type() const override { return _type; }
    virtual PointPtr fromPoint() const override { return _from_tx; }
    virtual PointPtr toPoint() const override { return _to_tx; }

   private:
    DepType _type;
    TxIDPtr _from_tx;
    TxIDPtr _to_tx;
};

DependenceSet TxIDTable::GetInDependence(uint64_t ts) {
    std::lock_guard<bthread::Mutex> lck(_m);

    if (_table.find(ts) != _table.end()) {
        return _table[ts]->GetInDependence();
    } else {
        LOG(WARNING) << "Fail to find ts: " << ts;
        return {};
    }
}

DependenceSet TxIDTable::GetOutDependence(uint64_t ts) {
    std::lock_guard<bthread::Mutex> lck(_m);

    if (_table.find(ts) != _table.end()) {
        return _table[ts]->GetOutDependence();
    } else {
        LOG(WARNING) << "Fail to find ts: " << ts;
        return {};
    }
}

PointSet TxIDTable::List() {
    std::lock_guard<bthread::Mutex> lck(_m);

    PointSet res;
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
    if (_table.find(ts2) == _table.end()) {
        LOG(ERROR) << "Fail to add dependency type: " << type << "ts1: " << ts1
                   << "ts2: " << ts2 << "(not found) ";
        return ENOENT;
    }

    DependencePtr p(new TxDependence(type, _table[ts1], _table[ts2]));

    _table[ts1]->add_out_dependence(p);
    _table[ts2]->add_in_dependence(p);
    return 0;
}

int TxIDTable::DelDep(const DependencePtr& dp) {
    std::lock_guard<bthread::Mutex> lck(_m);

    return _del_dep_locked(dp);
}

int TxIDTable::_del_dep_locked(const DependencePtr& dp) {
    auto ts1 = dp->fromPoint()->ID();
    auto ts2 = dp->toPoint()->ID();
    auto type = dp->Type();
    if (_table.find(ts1) == _table.end()) {
        LOG(ERROR) << "Fail to del dependency type: " << type << "ts1: " << ts1
                   << "(not found) "
                   << "ts2: " << ts2;
        return ENOENT;
    }
    if (_table.find(ts2) == _table.end()) {
        LOG(ERROR) << "Fail to del dependency type: " << type << "ts1: " << ts1
                   << "ts2: " << ts2 << "(not found) ";
        return ENOENT;
    }

    _table[ts1]->del_out_dependence(dp);
    _table[ts2]->del_in_dependence(dp);
    return 0;
}

int TxIDTable::DeleteTxID(const TxIdentifier& txid) {
    std::lock_guard<bthread::Mutex> lck(_m);

    if (_table.find(txid.start_ts()) == _table.end()) {
        return ENOENT;
    }

    auto tx = _table[txid.start_ts()];
    auto in_set = tx->GetInDependence();
    for (const auto& dp : in_set) {
        _del_dep_locked(dp);
    }
    auto out_set = tx->GetOutDependence();
    for (const auto& dp : out_set) {
        _del_dep_locked(dp);
    }

    _table.erase(txid.start_ts());
    if (txid.has_commit_ts()) {
        _table.erase(txid.commit_ts());
    }
    return 0;
}
}  // namespace txplanner
}  // namespace azino