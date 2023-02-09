#include "txid.h"

std::hash<uint64_t> hash;

#define LOCK2(p1, p2)                                             \
    if (p1->start_ts() < p2->start_ts()) {                        \
        p1->m.lock();                                             \
        p2->m.lock();                                             \
    } else {                                                      \
        p2->m.lock();                                             \
        p1->m.lock();                                             \
    }                                                             \
    std::lock_guard<bthread::Mutex> lck1(p1->m, std::adopt_lock); \
    std::lock_guard<bthread::Mutex> lck2(p2->m, std::adopt_lock);

#define LOCK3(p1, p2, p3)                                          \
    if (p1->start_ts() < p2->start_ts() < p3->start_ts()) {        \
        p1->m.lock();                                              \
        p2->m.lock();                                              \
        p3->m.lock();                                              \
    } else if (p1->start_ts() < p3->start_ts() < p2->start_ts()) { \
        p1->m.lock();                                              \
        p3->m.lock();                                              \
        p2->m.lock();                                              \
    } else if (p2->start_ts() < p1->start_ts() < p3->start_ts()) { \
        p2->m.lock();                                              \
        p1->m.lock();                                              \
        p3->m.lock();                                              \
    } else if (p3->start_ts() < p1->start_ts() < p2->start_ts()) { \
        p3->m.lock();                                              \
        p1->m.lock();                                              \
        p2->m.lock();                                              \
    } else if (p2->start_ts() < p3->start_ts() < p1->start_ts()) { \
        p2->m.lock();                                              \
        p3->m.lock();                                              \
        p1->m.lock();                                              \
    } else {                                                       \
        p3->m.lock();                                              \
        p2->m.lock();                                              \
        p1->m.lock();                                              \
    }                                                              \
    std::lock_guard<bthread::Mutex> lck1(p1->m, std::adopt_lock);  \
    std::lock_guard<bthread::Mutex> lck2(p2->m, std::adopt_lock);  \
    std::lock_guard<bthread::Mutex> lck3(p3->m, std::adopt_lock);

#define FINDABORTTXNONCONSECUTIVERWDEP(t1, t2, t3, res)            \
    if (t3->txid.status().status_code() !=                         \
        TxStatus_Code::TxStatus_Code_Commit) {                     \
        goto out;                                                  \
    }                                                              \
    if (t1->out.find(t2) == t1->out.end()) {                       \
        goto out;                                                  \
    }                                                              \
    if (t2->out.find(t3) == t2->out.end()) {                       \
        goto out;                                                  \
    }                                                              \
    if (t1->txid.status().status_code() == TxStatus_Code_Abort ||  \
        t2->txid.status().status_code() == TxStatus_Code_Abort) {  \
        goto out;                                                  \
    }                                                              \
                                                                   \
    if (t1->txid.status().status_code() == TxStatus_Code_Commit && \
        t1->txid.commit_ts() < t3->txid.commit_ts()) {             \
        goto out;                                                  \
    }                                                              \
    if (t2->txid.status().status_code() == TxStatus_Code_Commit && \
        t2->txid.commit_ts() < t3->txid.commit_ts()) {             \
        goto out;                                                  \
    }                                                              \
                                                                   \
    if (t1->txid.status().status_code() == TxStatus_Code_Commit) { \
        res.insert(t2);                                            \
    }                                                              \
    if (t2->txid.status().status_code() == TxStatus_Code_Commit) { \
        res.insert(t1);                                            \
    }

namespace azino {
namespace txplanner {
bool TxIDPtrEqual::operator()(const TxIDPtr& c1, const TxIDPtr& c2) const {
    return c1->start_ts() == c2->start_ts();
}

std::size_t TxIDPtrHash::operator()(const TxIDPtr& c) const {
    return hash(c->start_ts());
}
void TxID::early_validate() {
    if (early_validation_response && early_validation_done) {
        early_validation_response->set_allocated_txid(new TxIdentifier(txid));
        early_validation_done->Run();
    }
}

void TxID::ClearDep(const TxIDPtr& p) {
    std::unique_lock<bthread::Mutex> lck(p->m);
    auto save_in = p->in;
    auto save_out = p->out;
    lck.unlock();

    auto p1 = p;
    for (auto p2 : save_out) {
        TxID::DelDep(p1, p2);
    }

    auto p2 = p;
    for (auto p1 : save_in) {
        TxID::DelDep(p1, p2);
    }
}

void TxID::DelDep(const TxIDPtr& p1, const TxIDPtr& p2) {
    LOCK2(p1, p2)

    p1->out.erase(p2);
    p2->in.erase(p1);
}

void TxID::AddDep(DepType type, const TxIDPtr& p1, const TxIDPtr& p2) {
    LOCK2(p1, p2)

    if (p1->txid.status().status_code() == TxStatus_Code_Abort ||
        p2->txid.status().status_code() == TxStatus_Code_Abort) {
        LOG(WARNING) << "Fail to add dependency type: " << type
                     << "t1: " << p1->txid.ShortDebugString()
                     << "t2: " << p2->txid.ShortDebugString()
                     << "someone is aborted";
    }

    if (p1->txid.status().status_code() == TxStatus_Code_Commit &&
        p1->txid.commit_ts() < p2->txid.start_ts()) {
        LOG(WARNING) << "Fail to add dependency type: " << type
                     << "t1: " << p1->txid.ShortDebugString()
                     << "t2: " << p2->txid.ShortDebugString()
                     << "they are not concurrent";
    }

    p1->out.insert(p2);
    p2->in.insert(p1);
}

bool TxID::is_abort() {
    std::lock_guard<bthread::Mutex> lck(m);
    return txid.status().status_code() == TxStatus_Code_Abort;
}

bool TxID::is_commit() {
    std::lock_guard<bthread::Mutex> lck(m);
    return txid.status().status_code() == TxStatus_Code_Commit;
}

void TxID::add_early_validate(::azino::txplanner::ValidateTxResponse* response,
                              ::google::protobuf::Closure* done) {
    std::lock_guard<bthread::Mutex> lck(m);
    early_validation_response = response;
    early_validation_done = done;
}

TxIDPtr TxID::New(TimeStamp start_ts) {
    auto txstatus = new TxStatus();
    txstatus->set_status_code(TxStatus_Code_Start);

    TxIdentifier txid;
    txid.set_start_ts(start_ts);
    txid.set_allocated_status(txstatus);

    TxIDPtr p(new TxID);
    p->txid.CopyFrom(txid);
    p->_start_ts = start_ts;
    return p;
}

void TxID::commit(TimeStamp commit_ts) {
    std::lock_guard<bthread::Mutex> lck(m);

    if (txid.status().status_code() > TxStatus_Code_Preput) {
        // may happen, client will abort this tx
        LOG(INFO) << "unexpected status code when commit tx:"
                  << txid.ShortDebugString();
        return;
    }

    txid.set_commit_ts(commit_ts);
    txid.mutable_status()->set_status_code(TxStatus_Code_Commit);
}

void TxID::abort() {
    std::lock_guard<bthread::Mutex> lck(m);

    if (txid.status().status_code() > TxStatus_Code_Preput) {
        // should not happen
        LOG(ERROR) << "unexpected status code when abort tx:"
                   << txid.ShortDebugString();
        return;
    }

    txid.mutable_status()->set_status_code(TxStatus_Code_Abort);
}

TxIDPtrSet TxID::FindAbortTxnOnConsecutiveRWDep(const TxIDPtr& t1,
                                                const TxIDPtr& t2,
                                                const TxIDPtr& t3) {
    TxIDPtrSet res;
    if (t1->start_ts() == t3->start_ts()) {
        LOCK2(t1, t2);
        FINDABORTTXNONCONSECUTIVERWDEP(t1, t2, t3, res)
    } else {
        LOCK3(t1, t2, t3)
        FINDABORTTXNONCONSECUTIVERWDEP(t1, t2, t3, res)
    }

    if (res.size() == 2) {
        LOG(ERROR) << "find commit all three on consecutive rw dep t1 "
                   << t1->txid.ShortDebugString() << " t2 "
                   << t2->txid.ShortDebugString() << " t3 "
                   << t3->txid.ShortDebugString();
    }

out:
    return res;
}

TxIdentifier TxID::get_txid() {
    std::lock_guard<bthread::Mutex> lck(m);
    return txid;
}

TxIDPtrSet TxID::get_in() {
    std::lock_guard<bthread::Mutex> lck(m);
    return in;
}

TxIDPtrSet TxID::get_out() {
    std::lock_guard<bthread::Mutex> lck(m);
    return out;
}

}  // namespace txplanner
}  // namespace azino