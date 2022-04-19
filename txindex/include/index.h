#ifndef AZINO_TXINDEX_INCLUDE_INDEX_H
#define AZINO_TXINDEX_INCLUDE_INDEX_H

#include "azino/kv.h"
#include "service/tx.pb.h"
#include "service/kv.pb.h"
#include "gflags/gflags.h"

#include <functional>

DECLARE_int32(latch_bucket_num);

namespace azino {
namespace txindex {
    struct DataToPersist;
    class TxIndex {
    public:
        // return the default index impl
        static TxIndex* DefaultTxIndex();

        TxIndex() = default;

        TxIndex(const TxIndex&) = delete;
        TxIndex& operator=(const TxIndex&) = delete;

        virtual ~TxIndex() = default;

        // This is an atomic read-write operation for one user_key, only used in pessimistic transactions.
        // Success when no newer version of this key, intent or lock exists.
        // Should success if txid already hold this lock.
        virtual TxOpStatus WriteLock(const UserKey& key, const TxIdentifier& txid, std::function<void()> callback) = 0;

        // This is an atomic read-write operation for one user_key, used in both pessimistic and optimistic transactions.
        // Success when no newer version of this key, intent or lock exists.
        // Should success if txid already hold this intent or lock, and change lock to intent at the same time.
        virtual TxOpStatus WriteIntent(const UserKey& key, const Value& v, const TxIdentifier& txid) = 0;

        // This is an atomic read-write operation for one user_key, used in both pessimistic and optimistic transactions.
        // Success when it finds and cleans this tx's intent or lock for this key
        virtual TxOpStatus Clean(const UserKey& key, const TxIdentifier& txid) = 0;

        // This is an atomic read-write operation for one user_key, used in both pessimistic and optimistic transactions.
        // Success when it finds and commit this tx's intent for this key
        virtual TxOpStatus Commit(const UserKey& key, const TxIdentifier& txid) = 0;

        // Current implementation uses snapshot isolation.
        // read will be blocked if there exists and intent who has a smaller ts than read's ts.
        // read will bypass any lock, and return the key value pair who has the biggest ts among all that have ts smaller than read's ts.
        virtual TxOpStatus Read(const UserKey& key, Value& v, const TxIdentifier& txid, std::function<void()> callback) = 0;

        virtual TxOpStatus GetPersisting(uint32_t bucket_id, std::vector<DataToPersist> &datas) = 0;

        virtual TxOpStatus ClearPersisted(uint32_t bucket_id, const std::vector<DataToPersist> &datas) = 0;
    };
    struct DataToPersist {
        UserKey key;
        std::vector<std::pair<TimeStamp, Value *>> tvs;//Values are copied from origin and stored in heap, and ordered by ts descending
    };
} // namespace txindex
} // namespace azino

#endif // AZINO_TXINDEX_INCLUDE_INDEX_H
