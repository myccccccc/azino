#ifndef AZINO_TXINDEX_INCLUDE_INDEX_H
#define AZINO_TXINDEX_INCLUDE_INDEX_H

#include <functional>
#include <string>
#include <unordered_map>

#include "azino/kv.h"
#include "gflags/gflags.h"
#include "persistor.h"
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
typedef std::unordered_map<TimeStamp, TxIdentifier> ReaderMap;

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

class MVCCValue {
   public:
    MVCCValue() : _has_lock(false), _has_intent(false), _holder(), _t2v() {}
    DISALLOW_COPY_AND_ASSIGN(MVCCValue);
    ~MVCCValue() = default;
    inline bool HasLock() const { return _has_lock; }
    inline bool HasIntent() const { return _has_intent; }
    inline TxIdentifier Holder() const { return _holder; }
    inline txindex::ValuePtr IntentValue() const { return _intent_value; }
    inline size_t Size() const { return _t2v.size(); }
    void Lock(const TxIdentifier& txid);
    void Prewrite(const Value& v, const TxIdentifier& txid);
    void Clean();
    void Commit(const TxIdentifier& txid);

    std::pair<TimeStamp, txindex::ValuePtr> LargestTSValue() const;

    // Finds committed values whose timestamp is smaller or equal than "ts"
    std::pair<TimeStamp, txindex::ValuePtr> Seek(TimeStamp ts);

    // Finds committed values whose timestamp is bigger than "ts"
    std::pair<TimeStamp, txindex::ValuePtr> ReverseSeek(TimeStamp ts);

    // Truncate committed values whose timestamp is smaller or equal than "ts",
    // return the number of values truncated
    unsigned Truncate(TimeStamp ts);

    inline void AddReader(const TxIdentifier& txid) {
        _readers.insert(std::make_pair(txid.start_ts(), txid));
    }
    inline ReaderMap& Readers() { return _readers; }

    inline MultiVersionValue& MVV() { return _t2v; }

   private:
    bool _has_lock;
    bool _has_intent;
    TxIdentifier _holder;
    ValuePtr _intent_value;
    MultiVersionValue _t2v;
    ReaderMap _readers;
};

class KVBucket : public txindex::TxIndex {
   public:
    KVBucket() = default;
    DISALLOW_COPY_AND_ASSIGN(KVBucket);
    ~KVBucket() = default;

    virtual TxOpStatus WriteLock(const std::string& key,
                                 const TxIdentifier& txid,
                                 std::function<void()> callback,
                                 std::vector<txindex::Dep>& deps) override;
    virtual TxOpStatus WriteIntent(const std::string& key, const Value& v,
                                   const TxIdentifier& txid,
                                   std::vector<txindex::Dep>& deps) override;
    virtual TxOpStatus Clean(const std::string& key,
                             const TxIdentifier& txid) override;
    virtual TxOpStatus Commit(const std::string& key,
                              const TxIdentifier& txid) override;
    virtual TxOpStatus Read(const std::string& key, Value& v,
                            const TxIdentifier& txid,
                            std::function<void()> callback,
                            std::vector<txindex::Dep>& deps) override;
    virtual TxOpStatus GetPersisting(
        std::vector<txindex::DataToPersist>& datas) override;
    virtual TxOpStatus ClearPersisted(
        const std::vector<txindex::DataToPersist>& datas) override;

   private:
    std::unordered_map<std::string, MVCCValue> _kvs;
    std::unordered_map<std::string, std::vector<std::function<void()>>>
        _blocked_ops;
    bthread::Mutex _latch;
};

class TxIndexImpl : public txindex::TxIndex {
   public:
    TxIndexImpl(const std::string& storage_addr);
    DISALLOW_COPY_AND_ASSIGN(TxIndexImpl);
    ~TxIndexImpl();

    virtual TxOpStatus WriteLock(const std::string& key,
                                 const TxIdentifier& txid,
                                 std::function<void()> callback,
                                 std::vector<txindex::Dep>& deps) override;
    virtual TxOpStatus WriteIntent(const std::string& key, const Value& value,
                                   const TxIdentifier& txid,
                                   std::vector<txindex::Dep>& deps);
    virtual TxOpStatus Clean(const std::string& key, const TxIdentifier& txid);
    virtual TxOpStatus Commit(const std::string& key, const TxIdentifier& txid);
    virtual TxOpStatus Read(const std::string& key, Value& v,
                            const TxIdentifier& txid,
                            std::function<void()> callback,
                            std::vector<txindex::Dep>& deps);
    virtual TxOpStatus GetPersisting(
        std::vector<txindex::DataToPersist>& datas);
    virtual TxOpStatus ClearPersisted(
        const std::vector<txindex::DataToPersist>& datas);

   private:
    std::vector<KVBucket> _kvbs;
    txindex::Persistor _persistor;
    uint32_t _last_persist_bucket_num;
};

struct DataToPersist {
    std::string key;
    MultiVersionValue t2vs;
};

}  // namespace txindex
}  // namespace azino

#endif  // AZINO_TXINDEX_INCLUDE_INDEX_H
