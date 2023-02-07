

#include "index.h"

extern "C" void* CallbackWrapper(void* arg) {
    auto* func = reinterpret_cast<std::function<void()>*>(arg);
    func->operator()();
    delete func;
    return nullptr;
}

namespace azino {
namespace txindex {

void MVCCValue::Lock(const TxIdentifier& txid) {
    _has_lock = true;
    _holder.CopyFrom(txid);
}
void MVCCValue::Prewrite(const Value& v, const TxIdentifier& txid) {
    _has_lock = false;
    _has_intent = true;
    _holder.CopyFrom(txid);
    _intent_value.reset(new Value(v));
}
void MVCCValue::Clean() {
    _holder.Clear();
    _intent_value.reset();
    _has_intent = false;
    _has_lock = false;
}
void MVCCValue::Commit(const TxIdentifier& txid) {
    _holder.Clear();
    _t2v.insert(std::make_pair(txid.commit_ts(), std::move(_intent_value)));
    _has_intent = false;
    _has_lock = false;
}

MultiVersionValue::const_iterator MVCCValue::LargestTSValue() const {
    return _t2v.begin();
}

MultiVersionValue::const_iterator MVCCValue::Seek(TimeStamp ts) const {
    return _t2v.lower_bound(ts);
}

unsigned MVCCValue::Truncate(TimeStamp ts) {
    auto iter = _t2v.lower_bound(ts);
    auto ans = _t2v.size();
    _t2v.erase(iter, _t2v.end());
    return ans - _t2v.size();
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

}  // namespace txindex
}  // namespace azino