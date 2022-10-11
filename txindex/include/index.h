#ifndef AZINO_TXINDEX_INCLUDE_INDEX_H
#define AZINO_TXINDEX_INCLUDE_INDEX_H

#include <functional>
#include <string>

#include "azino/kv.h"
#include "gflags/gflags.h"
#include "reporter.h"
#include "service/kv.pb.h"
#include "service/tx.pb.h"

DECLARE_int32(latch_bucket_num);

namespace azino {
namespace txindex {

struct DataToPersist;
typedef std::shared_ptr<Value> ValuePtr;
typedef std::map<TimeStamp, ValuePtr, std::greater<TimeStamp>>
    MultiVersionValue;

class TxIndex {
   public:
    // return the default index impl
    static TxIndex* DefaultTxIndex(const std::string& storage_addr);

    TxIndex() = default;

    TxIndex(const TxIndex&) = delete;
    TxIndex& operator=(const TxIndex&) = delete;

    virtual ~TxIndex() = default;

    // This is an atomic read-write operation for one user_key, only used in
    // pessimistic transactions. Success when no newer version of this key,
    // intent or lock exists. Should success if txid already hold this lock.
    virtual TxOpStatus WriteLock(const std::string& key,
                                 const TxIdentifier& txid,
                                 std::function<void()> callback,
                                 std::vector<Dep>& deps) = 0;

    // This is an atomic read-write operation for one user_key, used in both
    // pessimistic and optimistic transactions. Success when no newer version of
    // this key, intent or lock exists. Should success if txid already hold this
    // intent or lock, and change lock to intent at the same time.
    virtual TxOpStatus WriteIntent(const std::string& key, const Value& value,
                                   const TxIdentifier& txid,
                                   std::vector<Dep>& deps) = 0;

    // This is an atomic read-write operation for one user_key, used in both
    // pessimistic and optimistic transactions. Success when it finds and cleans
    // this tx's intent or lock for this key
    virtual TxOpStatus Clean(const std::string& key,
                             const TxIdentifier& txid) = 0;

    // This is an atomic read-write operation for one user_key, used in both
    // pessimistic and optimistic transactions. Success when it finds and commit
    // this tx's intent for this key
    virtual TxOpStatus Commit(const std::string& key,
                              const TxIdentifier& txid) = 0;

    // Current implementation uses snapshot isolation.
    // read will be blocked if there exists and intent who has a smaller ts than
    // read's ts. read will bypass any lock, and return the key value pair who
    // has the biggest ts among all that have ts smaller than read's ts.
    virtual TxOpStatus Read(const std::string& key, Value& v,
                            const TxIdentifier& txid,
                            std::function<void()> callback,
                            std::vector<Dep>& deps) = 0;

    virtual TxOpStatus GetPersisting(std::vector<DataToPersist>& datas) = 0;

    virtual TxOpStatus ClearPersisted(
        const std::vector<DataToPersist>& datas) = 0;
};

struct DataToPersist {
    std::string key;
    MultiVersionValue t2vs;
};

}  // namespace txindex
}  // namespace azino

#endif  // AZINO_TXINDEX_INCLUDE_INDEX_H
