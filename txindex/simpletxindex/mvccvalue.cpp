

#include "index.h"

namespace azino {
namespace txindex {

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