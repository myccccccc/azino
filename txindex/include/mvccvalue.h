#ifndef AZINO_TXINDEX_INCLUDE_MVCCVALUE_H
#define AZINO_TXINDEX_INCLUDE_MVCCVALUE_H

#include <butil/macros.h>
#include <bvar/bvar.h>

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

enum MVCCLock {
    None = 0,
    WriteLock = 1,
    WriteIntent = 2,
};

class MVCCValue {
   public:
    MVCCValue();
    DISALLOW_COPY_AND_ASSIGN(MVCCValue);
    ~MVCCValue() = default;
    inline MVCCLock LockType() const { return _lock; }
    inline TxIdentifier LockHolder() const { return _lock_holder; }
    inline txindex::ValuePtr IntentValue() const { return _lock_value; }
    inline size_t Size() const { return _mvv.size(); }
    void Lock(const TxIdentifier& txid);
    void Prewrite(const Value& v, const TxIdentifier& txid);
    void Clean();
    void Commit(const TxIdentifier& txid);
    inline MultiVersionValue& MVV() { return _mvv; }
    void RecordWrite(bool err = false);
    double PessimismDegree();

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
    MVCCLock _lock;
    TxIdentifier _lock_holder;
    ValuePtr _lock_value;
    MultiVersionValue _mvv;
    ReaderMap _readers;
    std::vector<std::function<void()>> _waiters;

    // key metrics
    bvar::Adder<int> _write;  // total write num
    bvar::Window<bvar::Adder<int>> _write_window;
    bvar::Adder<int> _write_error;  // write error(conflict, too late) num
    bvar::Window<bvar::Adder<int>> _write_error_window;

    bvar::Adder<int> _tx_op_num;  // total tx operations number
    bvar::Window<bvar::Adder<int>> _tx_op_num_window;
    bvar::Adder<int>
        _tx_op_after_write_num;  // total tx operations after write number
    bvar::Window<bvar::Adder<int>> _tx_op_after_write_num_window;
};
}  // namespace txindex
}  // namespace azino
#endif  // AZINO_TXINDEX_INCLUDE_MVCCVALUE_H
