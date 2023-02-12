#ifndef AZINO_SDK_INCLUDE_TXWRITEBUFFER_H
#define AZINO_SDK_INCLUDE_TXWRITEBUFFER_H

#include <butil/macros.h>

#include <unordered_map>

#include "azino/kv.h"
#include "azino/options.h"
#include "service/kv.pb.h"

namespace azino {
enum TxWriteStatus { NONE = 0, LOCKED = 1, PREPUTED = 2, COMMITTED = 3 };

typedef struct TxWrite {
    WriteOptions options;
    Value value;
    TxWriteStatus status = NONE;
} TxWrite;

class TxWriteBuffer {
   public:
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

    std::unordered_map<UserKey, TxWrite>::iterator begin() {
        return _m.begin();
    }

    std::unordered_map<UserKey, TxWrite>::iterator end() { return _m.end(); }

    std::unordered_map<UserKey, TxWrite>::iterator find(const UserKey& key) {
        return _m.find(key);
    }

   private:
    std::unordered_map<UserKey, TxWrite> _m;
};
}  // namespace azino
#endif  // AZINO_SDK_INCLUDE_TXWRITEBUFFER_H
