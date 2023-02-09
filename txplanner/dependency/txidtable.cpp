#include "txidtable.h"

#include <gflags/gflags.h>

DEFINE_bool(enable_gc, true, "enable gc tx");

namespace azino {
namespace txplanner {

TxIDPtrSet TxIDTable::List() {
    std::lock_guard<bthread::Mutex> lck(_table_lock);

    TxIDPtrSet res;
    for (auto& iter : _table) {
        res.insert(iter.second);
    }
    return res;
}

std::pair<TxIDPtr, TxIDPtr> TxIDTable::AddDep(DepType type,
                                              const TxIdentifier& t1,
                                              const TxIdentifier& t2) {
    std::unique_lock<bthread::Mutex> lck(_table_lock);
    TxIDPtr p1, p2;

    auto iter1 = _table.find(t1.start_ts());
    auto iter2 = _table.find(t2.start_ts());

    if (iter1 == _table.end()) {
        LOG(WARNING) << "Fail to add dependency type: " << type
                   << "t1: " << t1.ShortDebugString() << "(not found) "
                   << "t2: " << t2.ShortDebugString();
        goto out;
    } else {
        p1 = iter1->second;
    }

    if (iter2 == _table.end()) {
        LOG(WARNING) << "Fail to add dependency type: " << type
                   << "t1: " << t1.ShortDebugString()
                   << "t2: " << t2.ShortDebugString() << "(not found) ";
        goto out;
    } else {
        p2 = iter2->second;
    }

    lck.unlock();

    TxID::AddDep(type, p1, p2);

out:
    return std::make_pair(p1, p2);
}

int TxIDTable::EarlyValidateTxID(
    const TxIdentifier& txid, ::azino::txplanner::ValidateTxResponse* response,
    ::google::protobuf::Closure* done) {
    std::unique_lock<bthread::Mutex> lck(_table_lock);
    TxIDPtr p;

    auto iter = _table.find(txid.start_ts());
    if (iter == _table.end()) {
        LOG(ERROR) << "Fail to validate TxID: " << txid.start_ts();
        return ENOENT;
    } else {
        p = iter->second;
    }

    lck.unlock();

    p->add_early_validate(response, done);

    return 0;
}

TxIDPtrSet TxIDTable::FindAbortTxnOnConsecutiveRWDep(TxIDPtr tx) {
    std::vector<std::vector<TxIDPtr>> deps;
    TxIDPtrSet res;

    {
        const auto& t3 = tx;
        if (t3->is_commit()) {
            for (const auto& t2 : t3->get_in()) {
                for (const auto& t1 : t2->get_in()) {
                    deps.push_back({t1, t2, t3});
                }
            }
        }
    }

    {
        const auto& t2 = tx;
        for (const auto& t3 : t2->get_out()) {
            if (t3->is_commit()) {
                for (const auto& t1 : t2->get_in()) {
                    deps.push_back({t1, t2, t3});
                }
            }
        }
    }

    {
        const auto& t1 = tx;
        for (const auto& t2 : t1->get_out()) {
            for (const auto& t3 : t2->get_out()) {
                deps.push_back({t1, t2, t3});
            }
        }
    }

    for (const auto& dep : deps) {
        auto tmp_res =
            TxID::FindAbortTxnOnConsecutiveRWDep(dep[0], dep[1], dep[2]);
        res.insert(tmp_res.begin(), tmp_res.end());
    }

    return res;
}

TxIDPtr TxIDTable::BeginTx(TimeStamp start_ts) {
    auto p = TxID::New(start_ts);
    add_tx(p);
    add_active_tx(p);

    return p;
}

TxIDPtr TxIDTable::CommitTx(const TxIdentifier& txid, TimeStamp commit_ts) {
    std::unique_lock<bthread::Mutex> lck(_table_lock);
    TxIDPtr p;

    auto iter = _table.find(txid.start_ts());
    if (iter == _table.end()) {
        LOG(FATAL) << "Fail to find tx when commit:" << txid.start_ts();
        return nullptr;
    } else {
        p = iter->second;
    }

    lck.unlock();

    p->commit(commit_ts);
    del_active_tx(p);

    return p;
}

TxIDPtr TxIDTable::AbortTx(const TxIdentifier& txid) {
    std::unique_lock<bthread::Mutex> lck(_table_lock);
    TxIDPtr p;

    auto iter = _table.find(txid.start_ts());
    if (iter == _table.end()) {
        LOG(FATAL) << "Fail to find tx when commit:" << txid.start_ts();
        return nullptr;
    } else {
        p = iter->second;
    }

    lck.unlock();

    p->abort();
    TxID::ClearDep(p);
    p->early_validate();
    del_active_tx(p);

    return p;
}

void TxIDTable::add_active_tx(TxIDPtr p) {
    std::lock_guard<bthread::Mutex> lck(_lock);

    _active_tx.push_back(p);
    _min_ats = (*_active_tx.begin())->start_ts();
    _max_ats = (*_active_tx.rbegin())->start_ts();
}

void TxIDTable::del_active_tx(TxIDPtr p) {
    std::lock_guard<bthread::Mutex> lck(_lock);

    while (!_active_tx.empty() && (*_active_tx.begin())->is_done()) {
        _done_tx.push_back(*_active_tx.begin());
        _active_tx.pop_front();
    }
    if (!_active_tx.empty()) {
        _min_ats = (*_active_tx.begin())->start_ts();
        _max_ats = (*_active_tx.rbegin())->start_ts();
    } else {
        _min_ats = MAX_TIMESTAMP;
        _max_ats = MIN_TIMESTAMP;
    }

    p->set_max_ats_when_done(_max_ats);
}

void TxIDTable::add_tx(TxIDPtr p) {
    std::lock_guard<bthread::Mutex> lck(_table_lock);
    _table.insert(std::make_pair(p->start_ts(), p));
}

void TxIDTable::del_tx(TxIDPtr p) {
    std::lock_guard<bthread::Mutex> lck(_table_lock);
    _table.erase(p->start_ts());
}

TxIDPtrSet TxIDTable::GCTx() {
    auto res = gc_inactive_tx();

    for (auto p : res) {
        del_tx(p);
        LOG(INFO) << "GC Tx " << p->get_txid().ShortDebugString();
    }

    return res;
}

TxIDPtrSet TxIDTable::gc_inactive_tx() {
    std::lock_guard<bthread::Mutex> lck(_lock);
    TxIDPtrSet res;

    while (!_done_tx.empty() && (*_done_tx.begin())->gc(_min_ats)) {
        res.insert(*_done_tx.begin());
        _done_tx.pop_front();
    }

    return res;
}

TxIDTable::TxIDTable() : gc(this) {
    if (FLAGS_enable_gc) {
        gc.Start();
    }
}

TxIDTable::~TxIDTable() { gc.Stop(); }

}  // namespace txplanner
}  // namespace azino