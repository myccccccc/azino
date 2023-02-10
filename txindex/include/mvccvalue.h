#ifndef AZINO_TXINDEX_INCLUDE_MVCCVALUE_H
#define AZINO_TXINDEX_INCLUDE_MVCCVALUE_H

#include <butil/macros.h>

#include <functional>
#include <string>
#include <unordered_map>

#include "azino/kv.h"
#include "gflags/gflags.h"
#include "service/kv.pb.h"
#include "service/tx.pb.h"

namespace azino {
namespace txindex {
typedef std::shared_ptr<Value> ValuePtr;
typedef struct TxIdentifierCmp {
    bool operator()(const TxIdentifier& lhs, const TxIdentifier& rhs) const {
        return lhs.commit_ts() > rhs.commit_ts();
    }
} TxIdentifierCmp;
typedef std::map<TxIdentifier, ValuePtr, TxIdentifierCmp> MultiVersionValue;
typedef std::unordered_map<TimeStamp, TxIdentifier> ReaderMap;

class MVCCValue {
   public:
    MVCCValue() : _has_lock(false), _has_intent(false), _holder(), _mvv() {}
    DISALLOW_COPY_AND_ASSIGN(MVCCValue);
    ~MVCCValue() = default;
    inline bool HasLock() const { return _has_lock; }
    inline bool HasIntent() const { return _has_intent; }
    inline TxIdentifier Holder() const { return _holder; }
    inline txindex::ValuePtr IntentValue() const { return _intent_value; }
    inline size_t Size() const { return _mvv.size(); }
    void Lock(const TxIdentifier& txid);
    void Prewrite(const Value& v, const TxIdentifier& txid);
    void Clean();
    void Commit(const TxIdentifier& txid);
    inline MultiVersionValue& MVV() { return _mvv; }

    MultiVersionValue::const_iterator LargestTSValue() const;

    // Finds committed values whose timestamp is smaller or equal than "ts"
    MultiVersionValue::const_iterator Seek(TimeStamp ts) const;
    // Finds committed values whose timestamp is smaller than "ts"
    MultiVersionValue::const_iterator Seek2(TimeStamp ts) const;

    // Truncate committed values whose timestamp is smaller or equal than "ts",
    // return the number of values truncated
    unsigned Truncate(const TxIdentifier& txid);

    inline void AddReader(const TxIdentifier& txid) {
        _readers.insert(std::make_pair(txid.start_ts(), txid));
    }
    inline ReaderMap& Readers() { return _readers; }

    inline void AddWaiter(const std::function<void()>& fn) {
        _waiters.push_back(fn);
    }
    void WakeUpWaiters();

   private:
    bool _has_lock;
    bool _has_intent;
    TxIdentifier _holder;
    ValuePtr _intent_value;
    MultiVersionValue _mvv;
    ReaderMap _readers;
    std::vector<std::function<void()>> _waiters;
};
}  // namespace txindex
}  // namespace azino
#endif  // AZINO_TXINDEX_INCLUDE_MVCCVALUE_H
