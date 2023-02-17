#ifndef AZINO_TXINDEX_INCLUDE_INDEX_H
#define AZINO_TXINDEX_INCLUDE_INDEX_H

#include <functional>
#include <string>
#include <unordered_map>

#include "azino/kv.h"
#include "azino/partition.h"
#include "azino/range.h"
#include "bthread/bthread.h"
#include "depedence.h"
#include "gflags/gflags.h"
#include "metric.h"
#include "mvccvalue.h"
#include "partition_manager.h"
#include "persist.h"
#include "service/kv.pb.h"
#include "service/tx.pb.h"

DECLARE_int32(latch_bucket_num);
DECLARE_string(txindex_addr);

/*
#define BTHREAD_RWLOCK_INIT(lock)                            \
    do {                                                     \
        int ec = bthread_rwlock_init(&lock, NULL);           \
        if (ec != 0) {                                       \
            throw std::system_error(                         \
                std::error_code(ec, std::system_category()), \
                "bthread_rwlock_init failed");               \
        }                                                    \
    } while (0);

#define BTHREAD_RWLOCK_RDLOCK(lock)                          \
    do {                                                     \
        int ec = bthread_rwlock_rdlock(&lock);               \
        if (ec != 0) {                                       \
            throw std::system_error(                         \
                std::error_code(ec, std::system_category()), \
                "bthread_rwlock_rdlock failed");             \
        }                                                    \
    } while (0);

#define BTHREAD_RWLOCK_WRLOCK(lock)                          \
    do {                                                     \
        int ec = bthread_rwlock_wrlock(&lock);               \
        if (ec != 0) {                                       \
            throw std::system_error(                         \
                std::error_code(ec, std::system_category()), \
                "bthread_rwlock_wrlock failed");             \
        }                                                    \
    } while (0);

#define BTHREAD_RWLOCK_UNLOCK(lock)                          \
    do {                                                     \
        int ec = bthread_rwlock_unlock(&lock);               \
        if (ec != 0) {                                       \
            throw std::system_error(                         \
                std::error_code(ec, std::system_category()), \
                "bthread_rwlock_unlock failed");             \
        }                                                    \
    } while (0);
*/

namespace azino {
namespace txindex {
class KVRegion;
typedef std::shared_ptr<KVRegion> KVRegionPtr;
typedef std::map<Range, KVRegionPtr, RangeComparator> RegionPartitionTable;

class TxIndex {
   public:
    TxIndex(brpc::Channel* txplaner_channel);
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
                                   const TxIdentifier& txid,
                                   std::function<void()> callback);

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
    KVRegionPtr route(const std::string& key);
    void init_region_table(const Partition& p);
    void init_storage(const Partition& p);

    brpc::Channel _storage_channel;
    brpc::Channel* _txplaner_channel;

    //    bthread_rwlock_t
    //        _region_table_lock;  // acquire read lock when route, acquire
    //        write lock
    //                             // when changing the range partition
    RegionPartitionTable _region_table;

    PartitionManager _pm;
};

class KVBucket {
   public:
    KVBucket() = default;
    DISALLOW_COPY_AND_ASSIGN(KVBucket);
    ~KVBucket() = default;

    TxOpStatus WriteLock(const std::string& key, const TxIdentifier& txid,
                         std::function<void()> callback, Deps& deps,
                         bool& is_lock_update);
    TxOpStatus WriteIntent(const std::string& key, const Value& v,
                           const TxIdentifier& txid,
                           std::function<void()> callback, Deps& deps,
                           bool& is_lock_update);
    TxOpStatus Clean(const std::string& key, const TxIdentifier& txid);
    TxOpStatus Commit(const std::string& key, const TxIdentifier& txid);
    TxOpStatus Read(const std::string& key, Value& v, const TxIdentifier& txid,
                    std::function<void()> callback, Deps& deps);
    int GetPersisting(std::vector<txindex::DataToPersist>& datas,
                      uint64_t min_ats);
    int ClearPersisted(const std::vector<txindex::DataToPersist>& datas);

   private:
    TxOpStatus Write(MVCCLock lock_type, const TxIdentifier& txid,
                     const std::string& key, const Value& v,
                     std::function<void()> callback, Deps& deps,
                     bool& is_lock_update);

    std::unordered_map<std::string, MVCCValue> _kvs;
    bthread::Mutex _latch;
};

class KVRegion {
   public:
    KVRegion(const Range& range, brpc::Channel* storage_channel,
             brpc::Channel* txplaner_channel);
    DISALLOW_COPY_AND_ASSIGN(KVRegion);
    ~KVRegion();

    TxOpStatus WriteLock(const std::string& key, const TxIdentifier& txid,
                         std::function<void()> callback);
    TxOpStatus WriteIntent(const std::string& key, const Value& value,
                           const TxIdentifier& txid,
                           std::function<void()> callback);
    TxOpStatus Clean(const std::string& key, const TxIdentifier& txid);
    TxOpStatus Commit(const std::string& key, const TxIdentifier& txid);
    TxOpStatus Read(const std::string& key, Value& v, const TxIdentifier& txid,
                    std::function<void()> callback);

    inline std::vector<KVBucket>& KVBuckets() { return _kvbs; }

    inline std::string Describe() { return _range.Describe(); }

   private:
    Range _range;
    std::vector<KVBucket> _kvbs;
    RegionPersist _persistor;
    Dependence _deprpt;
    RegionMetric _metric;
};

}  // namespace txindex
}  // namespace azino

#endif  // AZINO_TXINDEX_INCLUDE_INDEX_H
