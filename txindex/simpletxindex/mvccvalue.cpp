

#include "index.h"

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

std::pair<TimeStamp, txindex::ValuePtr> MVCCValue::LargestTSValue() const {
    if (_t2v.empty()) {
        return std::make_pair(MIN_TIMESTAMP, nullptr);
    }
    auto iter = _t2v.begin();
    return std::make_pair(iter->first, iter->second);
}

std::pair<TimeStamp, txindex::ValuePtr> MVCCValue::Seek(TimeStamp ts) {
    auto iter = _t2v.lower_bound(ts);
    if (iter == _t2v.end()) {
        return std::make_pair(MAX_TIMESTAMP, nullptr);
    }
    return std::make_pair(iter->first, iter->second);
}

std::pair<TimeStamp, txindex::ValuePtr> MVCCValue::ReverseSeek(TimeStamp ts) {
    auto iter = _t2v.lower_bound(ts);
    if (iter == _t2v.begin()) {
        return std::make_pair(MIN_TIMESTAMP, nullptr);
    }
    iter--;
    return std::make_pair(iter->first, iter->second);
}

unsigned MVCCValue::Truncate(TimeStamp ts) {
    auto iter = _t2v.lower_bound(ts);
    auto ans = _t2v.size();
    _t2v.erase(iter, _t2v.end());
    return ans - _t2v.size();
}

}  // namespace txindex
}  // namespace azino