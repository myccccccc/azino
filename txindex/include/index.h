#ifndef AZINO_TXINDEX_INCLUDE_INDEX_H
#define AZINO_TXINDEX_INCLUDE_INDEX_H

#include <functional>
#include <string>
#include <unordered_map>

#include "azino/kv.h"
#include "gflags/gflags.h"
#include "mvccvalue.h"
#include "persistor.h"
#include "reporter.h"
#include "service/kv.pb.h"
#include "service/tx.pb.h"

DECLARE_int32(latch_bucket_num);

namespace azino {
namespace txindex {
class KVRegion;

class TxIndex {
   public:
    TxIndex(brpc::Channel* storage_channel, brpc::Channel* txplaner_channel);
    TxIndex() = default;
    virtual ~TxIndex() = default;
    DISALLOW_ASSIGN(TxIndex);

    // This is an atomic read-write operation for one user_key, only used in
    // pessimistic transactions. Success when no newer version of this key,
    // intent or lock exists. Should success if txid already hold this lock.
    virtual TxOpStatus WriteLock(const std::string& key,
                                 const TxIdentifier& txid,
                                 std::function<void()> callback);

    // This is an atomic read-write operation for one user_key, used in both
    // pessimistic and optimistic transactions. Success when no newer version of
    // this key, intent or lock exists. Should success if txid already hold this
    // intent or lock, and change lock to intent at the same time.
    virtual TxOpStatus WriteIntent(const std::string& key, const Value& value,
                                   const TxIdentifier& txid);

    // This is an atomic read-write operation for one user_key, used in both
    // pessimistic and optimistic transactions. Success when it finds and cleans
    // this tx's intent or lock for this key
    virtual TxOpStatus Clean(const std::string& key, const TxIdentifier& txid);

    // This is an atomic read-write operation for one user_key, used in both
    // pessimistic and optimistic transactions. Success when it finds and commit
    // this tx's intent for this key
    virtual TxOpStatus Commit(const std::string& key, const TxIdentifier& txid);

    // Current implementation uses snapshot isolation.
    // read will be blocked if there exists and intent who has a smaller ts than
    // read's ts. read will bypass any lock, and return the key value pair who
    // has the biggest ts among all that have ts smaller than read's ts.
    virtual TxOpStatus Read(const std::string& key, Value& v,
                            const TxIdentifier& txid,
                            std::function<void()> callback);

   private:
    brpc::Channel* _storage_channel;
    brpc::Channel* _txplaner_channel;
    std::unique_ptr<KVRegion> _region;
};

class KVBucket {
   public:
    KVBucket() = default;
    DISALLOW_COPY_AND_ASSIGN(KVBucket);
    ~KVBucket() = default;

    TxOpStatus WriteLock(const std::string& key, const TxIdentifier& txid,
                         std::function<void()> callback, Deps& deps);
    TxOpStatus WriteIntent(const std::string& key, const Value& v,
                           const TxIdentifier& txid, Deps& deps);
    TxOpStatus Clean(const std::string& key, const TxIdentifier& txid);
    TxOpStatus Commit(const std::string& key, const TxIdentifier& txid);
    TxOpStatus Read(const std::string& key, Value& v, const TxIdentifier& txid,
                    std::function<void()> callback, Deps& deps);
    int GetPersisting(std::vector<txindex::DataToPersist>& datas);
    int ClearPersisted(const std::vector<txindex::DataToPersist>& datas);

   private:
    std::unordered_map<std::string, MVCCValue> _kvs;
    bthread::Mutex _latch;
};

class KVRegion : public txindex::TxIndex {
   public:
    KVRegion(brpc::Channel* storage_channel, brpc::Channel* txplaner_channel);
    DISALLOW_COPY_AND_ASSIGN(KVRegion);
    ~KVRegion();

    virtual TxOpStatus WriteLock(const std::string& key,
                                 const TxIdentifier& txid,
                                 std::function<void()> callback) override;
    virtual TxOpStatus WriteIntent(const std::string& key, const Value& value,
                                   const TxIdentifier& txid) override;
    virtual TxOpStatus Clean(const std::string& key,
                             const TxIdentifier& txid) override;
    virtual TxOpStatus Commit(const std::string& key,
                              const TxIdentifier& txid) override;
    virtual TxOpStatus Read(const std::string& key, Value& v,
                            const TxIdentifier& txid,
                            std::function<void()> callback) override;

    inline std::vector<KVBucket>& KVBuckets() { return _kvbs; }

   private:
    friend class Persistor;
    std::vector<KVBucket> _kvbs;
    Persistor _persistor;
    DepReporter _deprpt;
};

}  // namespace txindex
}  // namespace azino

#endif  // AZINO_TXINDEX_INCLUDE_INDEX_H
