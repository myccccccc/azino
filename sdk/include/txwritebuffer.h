#ifndef AZINO_SDK_INCLUDE_TXWRITEBUFFER_H
#define AZINO_SDK_INCLUDE_TXWRITEBUFFER_H

#include <butil/macros.h>

#include <unordered_map>

#include "azino/kv.h"
#include "azino/options.h"
#include "service/kv.pb.h"

namespace azino {
class TxWriteBuffer {
   public:
    typedef struct Write {
        WriteOptions options;
        std::shared_ptr<Value> value = nullptr;
        bool preput = false;
    } TxWrite;

    TxWriteBuffer() = default;
    DISALLOW_COPY_AND_ASSIGN(TxWriteBuffer);
    ~TxWriteBuffer() = default;

    // WriteBuffer will free value later
    void Write(const UserKey& key, Value* value, const WriteOptions options) {
        if (_m.find(key) == _m.end()) {
            _m.insert(std::make_pair(key, TxWrite()));
        }
        _m[key].value.reset(value);
        // if op1 write key1 pessimistic, then op2 write key1 optimistic
        // key1 is still write pessimistic
        _m[key].options.type = std::max(_m[key].options.type, options.type);
    }

    std::__detail::_Node_iterator<
        std::pair<const std::basic_string<char>, TxWrite>, false, true>
    begin() {
        return _m.begin();
    }

    std::__detail::_Node_iterator<
        std::pair<const std::basic_string<char>, TxWrite>, false, true>
    end() {
        return _m.end();
    }

    std::__detail::_Node_iterator<
        std::pair<const std::basic_string<char>, TxWrite>, false, true>
    find(const UserKey& key) {
        return _m.find(key);
    }

   private:
    std::unordered_map<UserKey, TxWrite> _m;
};
}  // namespace azino
#endif  // AZINO_SDK_INCLUDE_TXWRITEBUFFER_H
