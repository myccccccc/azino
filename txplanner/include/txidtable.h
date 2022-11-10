#ifndef AZINO_TXPLANNER_INCLUDE_TXIDTABLE_H
#define AZINO_TXPLANNER_INCLUDE_TXIDTABLE_H

#include <bthread/mutex.h>

#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "azino/kv.h"
#include "point.h"
#include "service/tx.pb.h"

namespace azino {
namespace txplanner {
class TxID;
typedef std::shared_ptr<TxID> TxIDPtr;

class TxIDTable : public DependenceGraph {
   public:
    TxIDTable() = default;
    virtual ~TxIDTable() = default;

    virtual DependenceSet GetInDependence(uint64_t id) override;

    virtual DependenceSet GetOutDependence(uint64_t id) override;

    virtual PointSet List() override;

    void UpsertTxID(const TxIdentifier& txid, TimeStamp start_ts,
                    TimeStamp commit_ts = MIN_TIMESTAMP);

    int DeleteTxID(const TxIdentifier& txid);

    int AddDep(DepType type, TimeStamp ts1, TimeStamp ts2);
    int DelDep(const DependencePtr& dp);

   private:
    bthread::Mutex _m;
    std::unordered_map<TimeStamp, TxIDPtr>
        _table;  // ts can be start_ts or commit_ts, their value must point at
                 // the same TxID
    int _del_dep_locked(const DependencePtr& dp);
};

}  // namespace txplanner
}  // namespace azino

#endif  // AZINO_TXPLANNER_INCLUDE_TXIDTABLE_H
