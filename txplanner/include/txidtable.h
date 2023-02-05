#ifndef AZINO_TXPLANNER_INCLUDE_TXIDTABLE_H
#define AZINO_TXPLANNER_INCLUDE_TXIDTABLE_H

#include <bthread/mutex.h>

#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "azino/kv.h"
#include "dependency.h"
#include "service/tx.pb.h"
#include "txid.h"

namespace azino {
namespace txplanner {

class TxIDTable {
   public:
    TxIDTable() = default;
    ~TxIDTable() = default;

    TxIDPtrSet GetInDependence(uint64_t id);

    TxIDPtrSet GetOutDependence(uint64_t id);

    TxIDPtrSet List();

    void UpsertTxID(const TxIdentifier& txid);

    int DeleteTxID(const TxIdentifier& txid);

    int AddDep(DepType type, TimeStamp ts1, TimeStamp ts2);

   private:
    bthread::Mutex _m;
    TxIDPtrMap _table;  // ts can be start_ts or commit_ts, their value must
                        // point at the same TxID
};

}  // namespace txplanner
}  // namespace azino

#endif  // AZINO_TXPLANNER_INCLUDE_TXIDTABLE_H
