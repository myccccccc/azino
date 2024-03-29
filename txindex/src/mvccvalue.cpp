#include "mvccvalue.h"

#include "bthread/bthread.h"

extern "C" void* CallbackWrapper(void* arg) {
    auto* func = reinterpret_cast<std::function<void()>*>(arg);
    func->operator()();
    delete func;
    return nullptr;
}

namespace azino {
namespace txindex {

void MVCCValue::Lock(const TxIdentifier& txid) {
    _lock = MVCCLock::WriteLock;
    _lock_holder.CopyFrom(txid);
}
void MVCCValue::Prewrite(const Value& v, const TxIdentifier& txid) {
    _lock = MVCCLock::WriteIntent;
    _lock_holder.CopyFrom(txid);
    _lock_value.reset(new Value(v));
}
void MVCCValue::Clean() {
    _lock = MVCCLock::None;
    _lock_holder.Clear();
    _lock_value.reset();
}
void MVCCValue::Commit(const TxIdentifier& txid) {
    _lock = MVCCLock::None;
    _lock_holder.Clear();
    _mvv.insert(std::make_pair(txid, std::move(_lock_value)));
}

MultiVersionValue::const_iterator MVCCValue::LargestTSValue() const {
    return _mvv.begin();
}

MultiVersionValue::const_iterator MVCCValue::Seek(TimeStamp ts) const {
    TxIdentifier tmp_tx;
    tmp_tx.set_commit_ts(ts);
    return _mvv.lower_bound(tmp_tx);
}

MultiVersionValue::const_iterator MVCCValue::Seek2(TimeStamp ts) const {
    TxIdentifier tmp_tx;
    tmp_tx.set_commit_ts(ts);
    return _mvv.upper_bound(tmp_tx);
}

unsigned MVCCValue::Truncate(const TxIdentifier& txid) {
    auto iter = _mvv.lower_bound(txid);
    auto ans = _mvv.size();
    _mvv.erase(iter, _mvv.end());
    return ans - _mvv.size();
}

void MVCCValue::WakeUpWaiters() {
    for (auto& func : _waiters) {
        bthread_t bid;
        auto* arg = new std::function<void()>(func);
        if (bthread_start_background(&bid, nullptr, CallbackWrapper, arg) !=
            0) {
            LOG(ERROR) << "Failed to start callback.";
        }
    }
    _waiters.clear();
}

MVCCValue::MVCCValue()
    : _lock(MVCCLock::None), _lock_holder(), _lock_value(), _mvv() {}

}  // namespace txindex
}  // namespace azino