#include "txidtable.h"

namespace azino {
namespace txplanner {

TxIDPtrSet TxIDTable::List() {
    std::lock_guard<bthread::Mutex> lck(_m);

    TxIDPtrSet res;
    for (auto& iter : _table) {
        res.insert(iter.second);
    }
    return res;
}

std::pair<TxIDPtr, TxIDPtr> TxIDTable::AddDep(DepType type,
                                              const TxIdentifier& t1,
                                              const TxIdentifier& t2) {
    std::lock_guard<bthread::Mutex> lck(_m);
    TxIDPtr p1, p2;

    if (_table.find(t1.start_ts()) == _table.end()) {
        LOG(ERROR) << "Fail to add dependency type: " << type
                   << "t1: " << t1.ShortDebugString() << "(not found) "
                   << "t2: " << t2.ShortDebugString();
        goto out;
    } else {
        p1 = _table[t1.start_ts()];
    }

    if (_table.find(t2.start_ts()) == _table.end()) {
        LOG(ERROR) << "Fail to add dependency type: " << type
                   << "t1: " << t1.ShortDebugString()
                   << "t2: " << t2.ShortDebugString() << "(not found) ";
        goto out;
    } else {
        p2 = _table[t2.start_ts()];
    }

    p1->out.insert(p2);
    p2->in.insert(p1);

out:
    return std::make_pair(p1, p2);
}

int TxIDTable::EarlyValidateTxID(
    const TxIdentifier& txid, ::azino::txplanner::ValidateTxResponse* response,
    ::google::protobuf::Closure* done) {
    std::lock_guard<bthread::Mutex> lck(_m);

    if (_table.find(txid.start_ts()) == _table.end()) {
        LOG(ERROR) << "Fail to validate TxID: " << txid.start_ts();
        return ENOENT;
    }

    auto tx = _table[txid.start_ts()];
    tx->early_validation_response = response;
    tx->early_validation_done = done;

    return 0;
}

TxIDPtrSet FindAbort(const TxIDPtr& t1, const TxIDPtr& t2, const TxIDPtr& t3) {
    TxIDPtrSet res;
    if (t3->txid.status().status_code() !=
        TxStatus_Code::TxStatus_Code_Commit) {
        return res;
    }
    if ((t1->txid.status().status_code() <=
             TxStatus_Code::TxStatus_Code_Preput ||
         t1->txid.commit_ts() >= t3->txid.commit_ts()) &&
        (t2->txid.status().status_code() <=
             TxStatus_Code::TxStatus_Code_Preput ||
         t2->txid.commit_ts() > t3->txid.commit_ts())) {
        if (t1->txid.status().status_code() == TxStatus_Code_Commit) {
            res.insert(t2);
        }
        if (t2->txid.status().status_code() == TxStatus_Code_Commit) {
            res.insert(t1);
        }
    }

    if (res.size() == 2) {
        LOG(ERROR) << "Committing all three ts1 " << t1->id() << " ts2 "
                   << t2->id() << " ts3 " << t3->id();
    }
    return res;
}

TxIDPtrSet TxIDTable::FindAbortTxnOnConsecutiveRWDep(TxIDPtr tx) {
    std::lock_guard<bthread::Mutex> lck(_m);
    TxIDPtrSet res;

    {
        auto t3 = tx;
        if (t3->txid.status().status_code() ==
            TxStatus_Code::TxStatus_Code_Commit) {
            for (auto t2 : t3->in) {
                for (auto t1 : t2->in) {
                    TxIDPtrSet res3 = FindAbort(t1, t2, t3);
                    res.insert(res3.begin(), res3.end());
                }
            }
        }
    }

    {
        auto t2 = tx;
        for (auto t3 : t2->out) {
            if (t3->txid.status().status_code() ==
                TxStatus_Code::TxStatus_Code_Commit) {
                for (auto t1 : t2->in) {
                    TxIDPtrSet res2 = FindAbort(t1, t2, t3);
                    res.insert(res2.begin(), res2.end());
                }
            }
        }
    }

    {
        auto t1 = tx;
        for (auto t2 : t1->out) {
            for (auto t3 : t2->out) {
                TxIDPtrSet res1 = FindAbort(t1, t2, t3);
                res.insert(res1.begin(), res1.end());
            }
        }
    }

    return res;
}

TxIDPtr TxIDTable::BeginTx(TimeStamp start_ts) {
    std::lock_guard<bthread::Mutex> lck(_m);

    auto txstatus = new TxStatus();
    txstatus->set_status_code(TxStatus_Code_Start);

    TxIdentifier txid;
    txid.set_start_ts(start_ts);
    txid.set_allocated_status(txstatus);

    TxIDPtr p(new TxID);
    _update_txid(p, txid);

    return p;
}

TxIDPtr TxIDTable::CommitTx(const TxIdentifier& txid, TimeStamp commit_ts) {
    std::lock_guard<bthread::Mutex> lck(_m);

    auto txstatus = new TxStatus;
    txstatus->CopyFrom(txid.status());
    txstatus->set_status_code(TxStatus_Code_Commit);

    TxIdentifier commit_txid;
    commit_txid.CopyFrom(txid);
    commit_txid.set_commit_ts(commit_ts);
    commit_txid.set_allocated_status(txstatus);

    auto p = _table.find(txid.start_ts())->second;
    if (p->txid.status().status_code() > TxStatus_Code_Preput) {
        LOG(ERROR) << "unexpected when commit tx:"
                   << p->txid.ShortDebugString();
        return p;
    }

    _update_txid(p, commit_txid);
    return p;
}

TxIDPtr TxIDTable::AbortTx(const TxIdentifier& txid) {
    std::lock_guard<bthread::Mutex> lck(_m);

    auto txstatus = new TxStatus;
    txstatus->CopyFrom(txid.status());
    txstatus->set_status_code(TxStatus_Code_Abort);

    TxIdentifier abort_txid;
    abort_txid.CopyFrom(txid);
    abort_txid.set_allocated_status(txstatus);

    auto p = _table.find(txid.start_ts())->second;
    if (p->txid.status().status_code() > TxStatus_Code_Abort) {
        LOG(ERROR) << "unexpected when abort tx:" << p->txid.ShortDebugString();
        return p;
    }

    _update_txid(p, abort_txid);
    p->del_dep(p);
    p->early_validate();
    return p;
}

void TxIDTable::_update_txid(TxIDPtr p, const TxIdentifier& txid) {
    p->txid.CopyFrom(txid);
    _table[txid.start_ts()] = p;
    if (txid.has_commit_ts()) {
        _table[txid.commit_ts()] = p;
    }
}

}  // namespace txplanner
}  // namespace azino