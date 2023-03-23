#ifndef AZINO_SDK_INCLUDE_TXRWBUFFER_H
#define AZINO_SDK_INCLUDE_TXRWBUFFER_H

#include <butil/macros.h>

#include <map>

#include "azino/comparator.h"
#include "azino/kv.h"
#include "azino/options.h"
#include "service/kv.pb.h"

namespace azino {
enum TxRWStatus { NONE = 0, LOCKED = 1, PREPUTED = 2, COMMITTED = 3 };
enum TxRWType { READ = 0, SERIALIZABLE_READ = 1, WRITE = 2 };

class TxRW {
   public:
    TxRW(TxRWType type, TxRWStatus status, bool is_delete = false,
         const UserValue& value = "")
        : _type(type), _status(status), _value() {
        _value.set_is_delete(is_delete);
        _value.set_content(value);
    };
    inline TxRWType& Type() { return _type; }
    inline TxRWStatus& Status() { return _status; }
    inline Value& Value() { return _value; }

   private:
    TxRWType _type = READ;
    TxRWStatus _status = NONE;
    class Value _value;
};

typedef std::map<UserKey, TxRW, BitWiseComparator> Buffer;

class TxRWBuffer {
   public:
    TxRWBuffer() = default;
    DISALLOW_COPY_AND_ASSIGN(TxRWBuffer);
    ~TxRWBuffer() = default;

    // WriteBuffer will free value later
    void Upsert(const UserKey& key, TxRW& new_tx_rw) {
        auto iter = _m.find(key);
        if (iter == _m.end()) {
            _m.insert(std::make_pair(key, new_tx_rw));
            return;
        }
        auto& tx_rw = iter->second;
        tx_rw.Type() = std::max(new_tx_rw.Type(), tx_rw.Type());
        tx_rw.Status() = std::max(new_tx_rw.Status(), tx_rw.Status());
        if (new_tx_rw.Type() == WRITE) {
            tx_rw.Value() = new_tx_rw.Value();
        }
    }

    Buffer::iterator begin() { return _m.begin(); }

    Buffer::iterator end() { return _m.end(); }

    Buffer::iterator find(const UserKey& key) { return _m.find(key); }

   private:
    Buffer _m;
};

}  // namespace azino
#endif  // AZINO_SDK_INCLUDE_TXRWBUFFER_H
