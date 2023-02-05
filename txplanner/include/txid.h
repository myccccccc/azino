#ifndef AZINO_TXPLANNER_INCLUDE_TXID_H
#define AZINO_TXPLANNER_INCLUDE_TXID_H

#include <bthread/mutex.h>

#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "azino/kv.h"
#include "service/tx.pb.h"

namespace azino {
namespace txplanner {
class TxIDTable;
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

class TxID {
   public:
    TxID() = default;
    ~TxID() = default;
    inline void reset_txid(const TxIdentifier& new_txid) { txid = new_txid; }

    inline uint64_t ID() const { return txid.start_ts(); }

   private:
    TxIdentifier txid;
    TxIDPtrSet in;
    TxIDPtrSet out;
    friend class TxIDTable;
};

}  // namespace txplanner
}  // namespace azino

#endif  // AZINO_TXPLANNER_INCLUDE_TXID_H
