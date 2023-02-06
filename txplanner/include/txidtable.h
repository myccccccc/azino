#ifndef AZINO_TXPLANNER_INCLUDE_TXIDTABLE_H
#define AZINO_TXPLANNER_INCLUDE_TXIDTABLE_H

#include <bthread/mutex.h>

#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "azino/kv.h"
#include "dependency.h"
#include "service/tx.pb.h"
#include "service/txplanner/txplanner.pb.h"
#include "txid.h"

namespace azino {
namespace txplanner {

class TxIDTable {
   public:
    TxIDTable() = default;
    ~TxIDTable() = default;

    TxIDPtrSet List();

    int AddDep(DepType type, TimeStamp ts1, TimeStamp ts2);

    int EarlyValidateTxID(const TxIdentifier& txid,
                          ::azino::txplanner::ValidateTxResponse* response,
                          ::google::protobuf::Closure* done);

    TxIDPtrSet FindAbortTxnOnConsecutiveRWDep(TimeStamp ts);

    TxIDPtr BeginTx(TimeStamp start_ts);
    TxIDPtr CommitTx(const TxIdentifier& txid, TimeStamp commit_ts);
    TxIDPtr AbortTx(const TxIdentifier& txid);

   private:
    void _update_txid(TxIDPtr p, const TxIdentifier& txid);
    bthread::Mutex _m;
    TxIDPtrMap _table;  // ts can be start_ts or commit_ts, their value must
                        // point at the same TxID
};

}  // namespace txplanner
}  // namespace azino

#endif  // AZINO_TXPLANNER_INCLUDE_TXIDTABLE_H
