#ifndef AZINO_TXPLANNER_INCLUDE_TXIDTABLE_H
#define AZINO_TXPLANNER_INCLUDE_TXIDTABLE_H

#include <bthread/mutex.h>

#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "azino/kv.h"
#include "dependency.h"
#include "gc.h"
#include "service/tx.pb.h"
#include "service/txplanner/txplanner.pb.h"
#include "txid.h"

namespace azino {
namespace txplanner {

class TxIDTable {
   public:
    TxIDTable();
    ~TxIDTable();

    TxIDPtrSet List();

    TxIDPtr BeginTx(TimeStamp start_ts);
    TxIDPtr CommitTx(const TxIdentifier& txid, TimeStamp commit_ts);
    TxIDPtr AbortTx(const TxIdentifier& txid);

    TxIDPtrSet GCTx();

    int EarlyValidateTxID(const TxIdentifier& txid,
                          ::azino::txplanner::ValidateTxResponse* response,
                          ::google::protobuf::Closure* done);

    TxIDPtrSet FindAbortTxnOnConsecutiveRWDep(TxIDPtr t);
    std::pair<TxIDPtr, TxIDPtr> AddDep(DepType type, const TxIdentifier& t1,
                                       const TxIdentifier& t2);

    TimeStamp GetMinATS();

   private:
    void add_tx(TxIDPtr p);
    void del_tx(TxIDPtr p);
    bthread::Mutex _table_lock;  // protect _table
    TxIDPtrMap _table;           // ts is start_ts

    void add_active_tx(TxIDPtr p);
    void del_active_tx(TxIDPtr p);
    TxIDPtrSet gc_inactive_tx();
    bthread::Mutex _lock;
    TxIDPtrQueue _active_tx;
    TxIDPtrQueue _done_tx;
    TimeStamp _min_ats = 0;
    TimeStamp _max_ats = 0;

    GC gc;
};

}  // namespace txplanner
}  // namespace azino

#endif  // AZINO_TXPLANNER_INCLUDE_TXIDTABLE_H
