#ifndef AZINO_SDK_INCLUDE_TXWRITEBUFFER_H
#define AZINO_SDK_INCLUDE_TXWRITEBUFFER_H

#include <butil/macros.h>

#include <unordered_map>

#include "azino/kv.h"
#include "azino/options.h"
#include "service/kv.pb.h"

namespace azino {
enum TxWriteStatus { NONE = 0, LOCKED = 1, PREPUTED = 2, COMMITTED = 3 };
class TxWriteBuffer {
   public:
    typedef struct Write {
        WriteOptions options;
        Value value;
        TxWriteStatus status = NONE;
    } TxWrite;

    TxWriteBuffer() = default;
    DISALLOW_COPY_AND_ASSIGN(TxWriteBuffer);
    ~TxWriteBuffer() = default;

    // WriteBuffer will free value later
    void Upsert(const WriteOptions options, const UserKey& key, bool is_delete,
                const UserValue& value) {
        if (_m.find(key) == _m.end()) {
            _m.insert(std::make_pair(key, TxWrite()));
        }
        _m[key].options = options;
        _m[key].value.set_is_delete(is_delete);
        _m[key].value.set_content(value);
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
