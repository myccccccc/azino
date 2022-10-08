#include <bthread/bthread.h>
#include <bthread/mutex.h>
#include <butil/containers/flat_map.h>
#include <butil/hash.h>
#include <gflags/gflags.h>

#include <functional>
#include <memory>
#include <unordered_map>

#include "index.h"
#include "persistor.h"

DEFINE_int32(latch_bucket_num, 1024, "latch buckets number");
DEFINE_bool(enable_persistor, false,
            "If enable persistor to persist data to storage server.");

extern "C" void* CallbackWrapper(void* arg) {
    auto* func = reinterpret_cast<std::function<void()>*>(arg);
    func->operator()();
    delete func;
    return nullptr;
}

// todo: need to consider the ltv in the storage
#define CHECK_WRITE_TOO_LATE(type)                                        \
    do {                                                                  \
        auto ltv = mv.LargestTSValue();                                   \
        if (ltv.first >= txid.start_ts()) {                               \
            ss << "Tx(" << txid.ShortDebugString() << ") write " << #type \
               << " on "                                                  \
               << "key: " << key << " too late. "                         \
               << "Find "                                                 \
               << "largest ts: " << ltv.first                             \
               << " value: " << ltv.second->ShortDebugString();           \
            sts.set_error_code(TxOpStatus_Code_WriteTooLate);             \
            sts.set_error_message(ss.str());                              \
            return sts;                                                   \
        }                                                                 \
    } while (0);

namespace azino {
namespace {

class MVCCValue {
   public:
    MVCCValue() : _has_lock(false), _has_intent(false), _holder(), _t2v() {}
    DISALLOW_COPY_AND_ASSIGN(MVCCValue);
    ~MVCCValue() = default;
    bool HasLock() const { return _has_lock; }
    bool HasIntent() const { return _has_intent; }
    TxIdentifier Holder() const { return _holder; }
    txindex::ValuePtr IntentValue() const { return _intent_value; }

    std::pair<TimeStamp, txindex::ValuePtr> LargestTSValue() const {
        if (_t2v.empty()) {
            return std::make_pair(MIN_TIMESTAMP, nullptr);
        }
        auto iter = _t2v.begin();
        return std::make_pair(iter->first, iter->second);
    }

    // Finds committed values whose timestamp is smaller or equal than "ts"
    std::pair<TimeStamp, txindex::ValuePtr> Seek(TimeStamp ts) {
        auto iter = _t2v.lower_bound(ts);
        if (iter == _t2v.end()) {
            return std::make_pair(MAX_TIMESTAMP, nullptr);
        }
        return std::make_pair(iter->first, iter->second);
    }

    // Truncate committed values whose timestamp is smaller or equal than "ts",
    // return the number of values truncated
    unsigned Truncate(TimeStamp ts) {
        auto iter = _t2v.lower_bound(ts);
        auto ans = _t2v.size();
        _t2v.erase(iter, _t2v.end());
        return ans - _t2v.size();
    }

   private:
    friend class KVBucket;
    bool _has_lock;
    bool _has_intent;
    TxIdentifier _holder;
    txindex::ValuePtr _intent_value;
    txindex::MultiVersionValue _t2v;
};

class KVBucket : public txindex::TxIndex {
   public:
    KVBucket() = default;
    DISALLOW_COPY_AND_ASSIGN(KVBucket);
    ~KVBucket() = default;

    virtual TxOpStatus WriteLock(const std::string& key,
                                 const TxIdentifier& txid,
                                 std::function<void()> callback) override {
        std::lock_guard<bthread::Mutex> lck(_latch);

        TxOpStatus sts;
        std::stringstream ss;

        MVCCValue& mv = _kvs[key];

        CHECK_WRITE_TOO_LATE(lock)

        if (mv.HasIntent() || mv.HasLock()) {
            assert(!(mv.HasIntent() && mv.HasLock()));

            if (txid.start_ts() != mv.Holder().start_ts()) {
                ss << "Tx(" << txid.ShortDebugString() << ") write lock on "
                   << "key: " << key << " blocked. "
                   << "Find " << (mv.HasLock() ? "lock" : "intent") << " Tx("
                   << mv.Holder().ShortDebugString() << ") value: "
                   << (mv.HasLock() ? ""
                                    : mv.IntentValue()->ShortDebugString());
                sts.set_error_code(TxOpStatus_Code_WriteBlock);
                sts.set_error_message(ss.str());
                _blocked_ops[key].push_back(callback);
                return sts;
            }

            ss << "Tx(" << txid.ShortDebugString() << ") write lock on "
               << "key: " << key << " repeated. "
               << "Find " << (mv.HasLock() ? "lock" : "intent") << " Tx("
               << mv.Holder().ShortDebugString() << ") value: "
               << (mv.HasLock() ? "" : mv.IntentValue()->ShortDebugString());
            sts.set_error_code(TxOpStatus_Code_Ok);
            sts.set_error_message(ss.str());
            LOG(WARNING) << ss.str();
            return sts;
        }

        mv._has_lock = true;
        mv._holder = txid;
        sts.set_error_code(TxOpStatus_Code_Ok);
        sts.set_error_message(ss.str());
        return sts;
    }

    virtual TxOpStatus WriteIntent(const std::string& key, const Value& v,
                                   const TxIdentifier& txid) override {
        std::lock_guard<bthread::Mutex> lck(_latch);

        TxOpStatus sts;
        std::stringstream ss;

        MVCCValue& mv = _kvs[key];

        CHECK_WRITE_TOO_LATE(intent)

        if (mv.HasIntent() || mv.HasLock()) {
            assert(!(mv.HasIntent() && mv.HasLock()));

            if (txid.start_ts() != mv.Holder().start_ts()) {
                ss << "Tx(" << txid.ShortDebugString() << ") write intent on "
                   << "key: " << key << " conflicts. "
                   << "Find " << (mv.HasLock() ? "lock" : "intent") << " Tx("
                   << mv.Holder().ShortDebugString() << ") value: "
                   << (mv.HasLock() ? ""
                                    : mv.IntentValue()->ShortDebugString());
                sts.set_error_code(TxOpStatus_Code_WriteConflicts);
                sts.set_error_message(ss.str());
                return sts;
            }

            if (mv.HasIntent()) {
                ss << "Tx(" << txid.ShortDebugString() << ") write intent on "
                   << "key: " << key << " repeated. "
                   << "Find "
                   << "intent"
                   << " Tx(" << mv.Holder().ShortDebugString()
                   << ") value: " << mv.IntentValue()->ShortDebugString();
                sts.set_error_code(TxOpStatus_Code_Ok);
                sts.set_error_message(ss.str());
                LOG(WARNING) << ss.str();
                return sts;
            } else {
                // go down
                ;
            }
        }

        mv._has_lock = false;
        mv._has_intent = true;
        mv._holder = txid;
        mv._intent_value.reset(new Value(v));
        sts.set_error_code(TxOpStatus_Code_Ok);
        sts.set_error_message(ss.str());
        return sts;
    }

    virtual TxOpStatus Clean(const std::string& key,
                             const TxIdentifier& txid) override {
        std::lock_guard<bthread::Mutex> lck(_latch);

        TxOpStatus sts;
        std::stringstream ss;

        MVCCValue& mv = _kvs[key];

        if ((!mv.HasLock() && !mv.HasIntent()) ||
            mv.Holder().start_ts() != txid.start_ts()) {
            ss << "Tx(" << txid.ShortDebugString() << ") clean on "
               << "key: " << key << " not exist. ";

            if (mv.HasLock() || mv.HasIntent()) {
                assert(!(mv.HasIntent() && mv.HasLock()));
                ss << "Find " << (mv.HasLock() ? "lock" : "intent") << " Tx("
                   << mv.Holder().ShortDebugString() << ") value: "
                   << (mv.HasLock() ? ""
                                    : mv.IntentValue()->ShortDebugString());
            }

            sts.set_error_code(TxOpStatus_Code_CleanNotExist);
            sts.set_error_message(ss.str());
            LOG(WARNING) << ss.str();
            return sts;
        }

        ss << "Tx(" << txid.ShortDebugString() << ") clean on "
           << "key: " << key << " success. "
           << "Find " << (mv.HasLock() ? "lock" : "intent") << " Tx("
           << mv.Holder().ShortDebugString() << ") value: "
           << (mv.HasLock() ? "" : mv.IntentValue()->ShortDebugString());

        sts.set_error_code(TxOpStatus_Code_Ok);
        sts.set_error_message(ss.str());

        mv._holder.Clear();
        mv._intent_value.reset();
        mv._has_intent = false;
        mv._has_lock = false;

        if (_blocked_ops.find(key) != _blocked_ops.end()) {
            auto iter = _blocked_ops.find(key);
            for (auto& func : iter->second) {
                bthread_t bid;
                auto* arg = new std::function<void()>(func);
                if (bthread_start_background(&bid, nullptr, CallbackWrapper,
                                             arg) != 0) {
                    LOG(ERROR) << "Failed to start callback.";
                }
            }
            iter->second.clear();
        }

        return sts;
    }

    virtual TxOpStatus Commit(const std::string& key,
                              const TxIdentifier& txid) override {
        std::lock_guard<bthread::Mutex> lck(_latch);

        TxOpStatus sts;
        std::stringstream ss;

        MVCCValue& mv = _kvs[key];

        if (!mv.HasIntent() || mv.Holder().start_ts() != txid.start_ts()) {
            ss << "Tx(" << txid.ShortDebugString() << ") commit on "
               << "key: " << key << " not exist. ";

            if (mv.HasLock() || mv.HasIntent()) {
                assert(!(mv.HasIntent() && mv.HasLock()));
                ss << "Find " << (mv.HasLock() ? "lock" : "intent") << " Tx("
                   << mv.Holder().ShortDebugString() << ") value: "
                   << (mv.HasLock() ? ""
                                    : mv.IntentValue()->ShortDebugString());
            }

            sts.set_error_code(TxOpStatus_Code_CommitNotExist);
            sts.set_error_message(ss.str());
            LOG(WARNING) << ss.str();
            return sts;
        }

        ss << "Tx(" << txid.ShortDebugString() << ") commit on "
           << "key: " << key << " success. "
           << "Find " << (mv.HasLock() ? "lock" : "intent") << " Tx("
           << mv.Holder().ShortDebugString() << ") value: "
           << (mv.HasLock() ? "" : mv.IntentValue()->ShortDebugString());
        sts.set_error_code(TxOpStatus_Code_Ok);
        sts.set_error_message(ss.str());

        mv._holder.Clear();
        mv._t2v.insert(
            std::make_pair(txid.commit_ts(), std::move(mv._intent_value)));
        mv._has_intent = false;
        mv._has_lock = false;

        if (_blocked_ops.find(key) != _blocked_ops.end()) {
            auto iter = _blocked_ops.find(key);
            for (auto& func : iter->second) {
                bthread_t bid;
                auto* arg = new std::function<void()>(func);
                if (bthread_start_background(&bid, nullptr, CallbackWrapper,
                                             arg) != 0) {
                    LOG(ERROR) << "Failed to start callback.";
                }
            }
            iter->second.clear();
        }

        return sts;
    }

    virtual TxOpStatus Read(const std::string& key, Value& v,
                            const TxIdentifier& txid,
                            std::function<void()> callback) override {
        std::lock_guard<bthread::Mutex> lck(_latch);

        TxOpStatus sts;
        std::stringstream ss;

        MVCCValue& mv = _kvs[key];

        if ((mv.HasIntent() || mv.HasLock()) &&
            mv.Holder().start_ts() == txid.start_ts()) {
            assert(!(mv.HasIntent() && mv.HasLock()));

            ss << "Tx(" << txid.ShortDebugString() << ") read on "
               << "key: " << key << " not exist. "
               << "Find it's own " << (mv.HasLock() ? "lock" : "intent")
               << " Tx(" << mv.Holder().ShortDebugString() << ") value: "
               << (mv.HasLock() ? "" : mv.IntentValue()->ShortDebugString());
            sts.set_error_code(TxOpStatus_Code_ReadNotExist);
            sts.set_error_message(ss.str());
            LOG(ERROR) << ss.str();
            return sts;
        }

        if (mv.HasIntent() && mv.Holder().start_ts() < txid.start_ts()) {
            assert(!mv.HasLock());

            ss << "Tx(" << txid.ShortDebugString() << ") read on "
               << "key: " << key << " blocked. "
               << "Find "
               << "intent"
               << " Tx(" << mv.Holder().ShortDebugString()
               << ") value: " << mv.IntentValue()->ShortDebugString();
            sts.set_error_code(TxOpStatus_Code_ReadBlock);
            sts.set_error_message(ss.str());
            _blocked_ops[key].push_back(callback);
            return sts;
        }

        auto sv = mv.Seek(txid.start_ts());
        if (sv.first <= txid.start_ts()) {
            ss << "Tx(" << txid.ShortDebugString() << ") read on "
               << "key: " << key << " success. "
               << "Find "
               << "ts: " << sv.first
               << " value: " << sv.second->ShortDebugString();
            sts.set_error_code(TxOpStatus_Code_Ok);
            sts.set_error_message(ss.str());
            v.CopyFrom(*(sv.second));
            return sts;
        }

        ss << "Tx(" << txid.ShortDebugString() << ") read on "
           << "key: " << key << " not exist. ";
        sts.set_error_code(TxOpStatus_Code_ReadNotExist);
        sts.set_error_message(ss.str());
        return sts;
    }

    virtual TxOpStatus GetPersisting(
        std::vector<txindex::DataToPersist>& datas) override {
        std::lock_guard<bthread::Mutex> lck(_latch);

        TxOpStatus sts;
        std::stringstream ss;
        unsigned long cnt = 0;
        for (auto& it : _kvs) {
            if (it.second._t2v.empty()) {
                continue;
            }
            txindex::DataToPersist d;
            d.key = it.first;
            d.t2vs = it.second._t2v;
            cnt += it.second._t2v.size();
            datas.push_back(d);
        }
        if (cnt == 0) {
            sts.set_error_code(TxOpStatus_Code_NoneToPersist);
        } else {
            sts.set_error_code(TxOpStatus_Code_Ok);
        }

        ss << "Get data to persist. "
           << "Persist key num: " << datas.size()
           << "Persist value num: " << cnt;
        sts.set_error_message(ss.str());
        LOG(INFO) << ss.str();
        return sts;
    }

    virtual TxOpStatus ClearPersisted(
        const std::vector<txindex::DataToPersist>& datas) override {
        std::lock_guard<bthread::Mutex> lck(_latch);

        TxOpStatus sts;
        std::stringstream ss;
        unsigned long cnt = 0;
        for (auto& it : datas) {
            assert(!it.t2vs.empty());
            if (_kvs.find(it.key) == _kvs.end()) {
                sts.set_error_code(TxOpStatus_Code_ClearRepeat);
                ss << "UserKey: " << it.key
                   << "repeat clear due to no key in _kvs.";
                break;
            }
            auto n = _kvs[it.key].Truncate(it.t2vs.begin()->first);
            cnt += n;
            if (it.t2vs.size() != n) {
                sts.set_error_code(TxOpStatus_Code_ClearRepeat);
                ss << "UserKey: " << it.key
                   << "repeat clear due to truncate number not match.";
                break;
            }
        }
        if (sts.error_code() == TxOpStatus_Code_Ok) {
            ss << "Clear persisted data success. "
               << "Clear persist key num: " << datas.size()
               << "CLear persist value num: " << cnt;
            sts.set_error_message(ss.str());
            LOG(INFO) << ss.str();
        } else {
            sts.set_error_message(ss.str());
            LOG(ERROR) << ss.str();
        }
        return sts;
    }

   private:
    std::unordered_map<std::string, MVCCValue> _kvs;
    std::unordered_map<std::string, std::vector<std::function<void()>>>
        _blocked_ops;
    bthread::Mutex _latch;
};

class TxIndexImpl : public txindex::TxIndex {
   public:
    TxIndexImpl(const std::string& storage_addr)
        : _kvbs(FLAGS_latch_bucket_num),
          _persistor(this, storage_addr),
          _last_persist_bucket_num(0) {
        if (FLAGS_enable_persistor) {
            _persistor.Start();
        }
    }
    DISALLOW_COPY_AND_ASSIGN(TxIndexImpl);
    ~TxIndexImpl() {
        if (FLAGS_enable_persistor) {
            _persistor.Stop();
        }
    }

    virtual TxOpStatus WriteLock(const std::string& key,
                                 const TxIdentifier& txid,
                                 std::function<void()> callback) override {
        // todo: use a wrapper, and another hash function
        auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
        return _kvbs[bucket_num].WriteLock(key, txid, callback);
    }

    virtual TxOpStatus WriteIntent(const std::string& key, const Value& value,
                                   const TxIdentifier& txid) override {
        // todo: use a wrapper, and another hash function
        auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
        return _kvbs[bucket_num].WriteIntent(key, value, txid);
    }

    virtual TxOpStatus Clean(const std::string& key,
                             const TxIdentifier& txid) override {
        // todo: use a wrapper, and another hash function
        auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
        return _kvbs[bucket_num].Clean(key, txid);
    }

    virtual TxOpStatus Commit(const std::string& key,
                              const TxIdentifier& txid) override {
        // todo: use a wrapper, and another hash function
        auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
        return _kvbs[bucket_num].Commit(key, txid);
    }

    virtual TxOpStatus Read(const std::string& key, Value& v,
                            const TxIdentifier& txid,
                            std::function<void()> callback) override {
        // todo: use a wrapper, and another hash function
        auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
        return _kvbs[bucket_num].Read(key, v, txid, callback);
    }

    virtual TxOpStatus GetPersisting(
        std::vector<txindex::DataToPersist>& datas) override {
        TxOpStatus res;
        for (int i = 0; i < FLAGS_latch_bucket_num; i++) {
            _last_persist_bucket_num++;
            res = _kvbs[_last_persist_bucket_num % FLAGS_latch_bucket_num]
                      .GetPersisting(datas);
        }
        return res;
    }

    virtual TxOpStatus ClearPersisted(
        const std::vector<txindex::DataToPersist>& datas) override {
        return _kvbs[_last_persist_bucket_num % FLAGS_latch_bucket_num]
            .ClearPersisted(datas);
    }

   private:
    std::vector<KVBucket> _kvbs;
    txindex::Persistor _persistor;
    uint32_t _last_persist_bucket_num;
};

}  // namespace

namespace txindex {
TxIndex* TxIndex::DefaultTxIndex(const std::string& storage_addr) {
    return new TxIndexImpl(storage_addr);
}
}  // namespace txindex

}  // namespace azino