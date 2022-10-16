#ifndef AZINO_TXPLANNER_INCLUDE_TXIDTABLE_H
#define AZINO_TXPLANNER_INCLUDE_TXIDTABLE_H

#include <bthread/mutex.h>

#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "azino/kv.h"
#include "dependency.h"
#include "service/tx.pb.h"

namespace azino {
namespace txplanner {
class TxID;

typedef std::shared_ptr<TxID> TxIDPtr;

class TxIDTable : public DependenceGraph {
   public:
    TxIDTable() = default;
    virtual ~TxIDTable() = default;

    virtual DependenceSet GetInDependence(TimeStamp ts) override;

    virtual DependenceSet GetOutDependence(TimeStamp ts) override;

    virtual IDSet ListID() override;

    void UpsertTxID(const TxIdentifier& txid, TimeStamp start_ts,
                    TimeStamp commit_ts = MIN_TIMESTAMP);

    int AddDep(DepType type, TimeStamp ts1, TimeStamp ts2);

   private:
    bthread::Mutex _m;
    std::unordered_map<TimeStamp, TxIDPtr>
        _table;  // ts can be start_ts or commit_ts, their value must point at
                 // the same TxID
};

}  // namespace txplanner
}  // namespace azino

#endif  // AZINO_TXPLANNER_INCLUDE_TXIDTABLE_H
