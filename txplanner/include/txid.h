#ifndef AZINO_TXPLANNER_INCLUDE_TXID_H
#define AZINO_TXPLANNER_INCLUDE_TXID_H

#include <bthread/mutex.h>

#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "azino/kv.h"
#include "dependency.h"
#include "service/tx.pb.h"
#include "service/txplanner/txplanner.pb.h"

namespace azino {
namespace txplanner {
class TxID;

typedef std::shared_ptr<TxID> TxIDPtr;

class TxIDPtrEqual {
   public:
    bool operator()(const TxIDPtr& c1, const TxIDPtr& c2) const;
};

class TxIDPtrHash {
   public:
    std::size_t operator()(const TxIDPtr& c) const;
};

typedef std::unordered_map<TimeStamp, TxIDPtr> TxIDPtrMap;
typedef std::unordered_set<TxIDPtr, TxIDPtrHash, TxIDPtrEqual> TxIDPtrSet;
typedef std::deque<TxIDPtr> TxIDPtrQueue;

class TxID {
   public:
    static TxIDPtr New(TimeStamp start_ts);
    ~TxID() = default;

    int commit(TimeStamp commit_ts);
    int abort();

    inline bool is_done() { return is_abort() || is_commit(); }
    inline uint64_t start_ts() { return _start_ts; }
    inline int64_t begin_time() { return _begin_time; }
    bool is_abort();
    bool is_commit();
    inline bool gc(TimeStamp min_ats) {
        return finished_by_client && min_ats > max_ats_when_done;
    }
    TxIdentifier get_txid();
    TxIDPtrSet get_in();
    TxIDPtrSet get_out();

    void add_early_validate(::azino::txplanner::ValidateTxResponse* response,
                            ::google::protobuf::Closure* done);
    void early_validate();

    static void AddDep(DepType type, const TxIDPtr& p1, const TxIDPtr& p2);
    static void DelDep(const TxIDPtr& p1, const TxIDPtr& p2);
    static void ClearDep(const TxIDPtr& p);
    static TxIDPtrSet FindAbortTxnOnConsecutiveRWDep(const TxIDPtr& p1,
                                                     const TxIDPtr& p2,
                                                     const TxIDPtr& p3);

    inline void set_max_ats_when_done(TimeStamp ts) { max_ats_when_done = ts; }
    inline void set_finished_by_client() { finished_by_client = true; }

   private:
    TxID() = default;
    uint64_t _start_ts;
    int64_t _begin_time;
    bthread::Mutex m;
    TxIdentifier txid;
    TxIDPtrSet in;
    TxIDPtrSet out;
    TimeStamp max_ats_when_done = MAX_TIMESTAMP;
    ::azino::txplanner::ValidateTxResponse* early_validation_response = nullptr;
    ::google::protobuf::Closure* early_validation_done = nullptr;
    bool finished_by_client = false;  // explict commit/abort by client
};

}  // namespace txplanner
}  // namespace azino

#endif  // AZINO_TXPLANNER_INCLUDE_TXID_H
