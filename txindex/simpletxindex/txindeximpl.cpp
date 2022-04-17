#include <bthread/mutex.h>
#include <gflags/gflags.h>
#include <butil/hash.h>
#include <butil/containers/flat_map.h>
#include <functional>
#include <memory>
#include <unordered_map>
#include <bthread/bthread.h>

#include "index.h"

DEFINE_int32(latch_bucket_num, 1024, "latch buckets number");

extern "C" void* CallbackWrapper(void* arg) {
    auto* func = reinterpret_cast<std::function<void()>*>(arg);
    func->operator()();
    delete func;
    return nullptr;
}

namespace azino {
namespace {

class MVCCValue {
public:
    MVCCValue() :
    _has_lock(false),
    _has_intent(false),
    _holder(), _t2v() {}
    DISALLOW_COPY_AND_ASSIGN(MVCCValue);
    ~MVCCValue() = default;
    bool HasLock() const { return _has_lock; }
    bool HasIntent() const { return _has_intent; }
    std::pair<TimeStamp, Value*> LargestTSValue() const {
       if (_t2v.empty()) {
           return std::make_pair(MIN_TIMESTAMP, nullptr);
       }
       auto iter = _t2v.begin();
       return std::make_pair(iter->first, iter->second.get());
    }
    TxIdentifier Holder() const { return _holder; }
    Value* IntentValue() const {
        return _intent_value.get();
    }
    // finds greater or equal
    std::pair<TimeStamp, Value*> Seek(TimeStamp ts) {
        auto iter = _t2v.lower_bound(ts);
        if (iter == _t2v.end()) {
            return std::make_pair(MAX_TIMESTAMP, nullptr);
        }
        return std::make_pair(iter->first, iter->second.get());
    }

private:
    friend class KVBucket;
    bool _has_lock;
    bool _has_intent;
    std::unique_ptr<Value> _intent_value;
    TxIdentifier _holder;
    std::map<TimeStamp, std::unique_ptr<Value>, std::greater<TimeStamp>> _t2v;
};

class KVBucket : public txindex::TxIndex {
public:
    KVBucket() = default;
    DISALLOW_COPY_AND_ASSIGN(KVBucket);
    ~KVBucket() = default;

    virtual TxOpStatus WriteLock(const UserKey& key, const TxIdentifier& txid, std::function<void()> callback) override {
        std::lock_guard<bthread::Mutex> lck(_latch);

        TxOpStatus sts;
        std::stringstream ss;
        if (_kvs.find(key) == _kvs.end()) {
           _kvs.insert(std::make_pair(key, new MVCCValue()));
        }
        MVCCValue* mv = _kvs[key].get();
        auto ltv = mv->LargestTSValue();

        if (ltv.first >= txid.start_ts()) {
            ss << "Tx(" << txid.ShortDebugString() << ") write lock on " << "key: "<< key << " too late. "
               << "Find " << "largest ts: " << ltv.first << " value: "
               << ltv.second->ShortDebugString();
            sts.set_error_code(TxOpStatus_Code_WriteTooLate);
            sts.set_error_message(ss.str());
            LOG(INFO) << ss.str();
            return sts;
        }

        if (mv->HasIntent() || mv->HasLock()) {
            assert(!(mv->HasIntent() && mv->HasLock()));
            if (txid.start_ts() != mv->Holder().start_ts()) {
                ss << "Tx(" << txid.ShortDebugString() << ") write lock on " << "key: "<< key << " blocked. "
                   << "Find " << (mv->HasLock() ? "lock" : "intent") << " Tx(" << mv->Holder().ShortDebugString() << ") value: "
                   << (mv->HasLock() ? "" : mv->IntentValue()->ShortDebugString());
                sts.set_error_code(TxOpStatus_Code_WriteBlock);
                sts.set_error_message(ss.str());
                LOG(INFO) << ss.str();
                if (_blocked_ops.find(key) == _blocked_ops.end()) {
                    _blocked_ops.insert(std::make_pair(key, std::vector<std::function<void()>>()));
                }
                _blocked_ops[key].push_back(callback);
                return sts;
            }
            ss << "Tx(" << txid.ShortDebugString() << ") write lock on " << "key: "<< key << " repeated. "
               << "Find "<< (mv->HasLock() ? "lock" : "intent") << " Tx(" << mv->Holder().ShortDebugString() << ") value: "
               << (mv->HasLock() ? "" : mv->IntentValue()->ShortDebugString());
            sts.set_error_code(TxOpStatus_Code_Ok);
            sts.set_error_message(ss.str());
            LOG(NOTICE) << ss.str();
            return sts;
        }

        mv->_has_lock = true;
        mv->_holder = txid;
        ss << "Tx(" << txid.ShortDebugString() << ") write lock on " << "key: "<< key << " successes. ";
        sts.set_error_code(TxOpStatus_Code_Ok);
        sts.set_error_message(ss.str());
        LOG(INFO) << ss.str();
        return sts;
    }

    virtual TxOpStatus WriteIntent(const UserKey& key, const Value& v, const TxIdentifier& txid) override {
        std::lock_guard<bthread::Mutex> lck(_latch);

        TxOpStatus sts;
        std::stringstream ss;
        if (_kvs.find(key) == _kvs.end()) {
            _kvs.insert(std::make_pair(key, new MVCCValue()));
        }
        MVCCValue* mv = _kvs[key].get();
        auto ltv = mv->LargestTSValue();

        if (ltv.first >= txid.start_ts()) {
            ss << "Tx(" << txid.ShortDebugString() << ") write intent on " << "key: "<< key << " too late. "
               << "Find " << "largest ts: " << ltv.first << " value: "
               << ltv.second->ShortDebugString();
            sts.set_error_code(TxOpStatus_Code_WriteTooLate);
            sts.set_error_message(ss.str());
            LOG(INFO) << ss.str();
            return sts;
        }

        if (mv->HasIntent() || mv->HasLock()) {
            assert(!(mv->HasIntent() && mv->HasLock()));
            if (txid.start_ts() != mv->Holder().start_ts()) {
                ss << "Tx(" << txid.ShortDebugString() << ") write intent on " << "key: "<< key << " conflicts. "
                   << "Find " << (mv->HasLock() ? "lock" : "intent") << " Tx(" << mv->Holder().ShortDebugString() << ") value: "
                   << (mv->HasLock() ? "" : mv->IntentValue()->ShortDebugString());
                sts.set_error_code(TxOpStatus_Code_WriteConflicts);
                sts.set_error_message(ss.str());
                LOG(INFO) << ss.str();
                return sts;
            }
            if (mv->HasIntent()) {
                ss << "Tx(" << txid.ShortDebugString() << ") write intent on " << "key: "<< key << " repeated. "
                   << "Find "<< "intent" << " Tx(" << mv->Holder().ShortDebugString() << ") value: "
                   << mv->IntentValue()->ShortDebugString();
                sts.set_error_code(TxOpStatus_Code_Ok);
                sts.set_error_message(ss.str());
                LOG(NOTICE) << ss.str();
                return sts;
            }

            mv->_has_lock = false;
            mv->_has_intent = true;
            mv->_holder = txid;
            mv->_intent_value.reset(new Value(v));
            ss << "Tx(" << txid.ShortDebugString() << ") write intent on " << "key: "<< key << " successes. "
               << "Find "<< "lock" << " Tx(" << mv->Holder().ShortDebugString() << ") value: ";
            sts.set_error_code(TxOpStatus_Code_Ok);
            sts.set_error_message(ss.str());
            LOG(INFO) << ss.str();
            return sts;
        }

        mv->_has_intent = true;
        mv->_holder = txid;
        mv->_intent_value.reset(new Value(v));
        ss << "Tx(" << txid.ShortDebugString() << ") write intent on " << "key: "<< key << " successes. ";
        sts.set_error_code(TxOpStatus_Code_Ok);
        sts.set_error_message(ss.str());
        LOG(INFO) << ss.str();
        return sts;
    }

    virtual TxOpStatus Clean(const UserKey& key, const TxIdentifier& txid) override {
        std::lock_guard<bthread::Mutex> lck(_latch);

        TxOpStatus sts;
        std::stringstream ss;
        auto iter = _kvs.find(key);
        if (iter == _kvs.end()
            || (!iter->second->HasLock() && !iter->second->HasIntent())
            || iter->second->Holder().start_ts() != txid.start_ts()) {
            ss << "Tx(" << txid.ShortDebugString() << ") clean on " << "key: "<< key << " not exist. ";
            if (iter != _kvs.end()) {
                assert(!(iter->second->HasIntent() && iter->second->HasLock()));
                ss << "Find " << (iter->second->HasLock() ? "lock" : "intent") << " Tx(" << iter->second->Holder().ShortDebugString() << ") value: "
                   << (iter->second->HasLock() ? "" : iter->second->IntentValue()->ShortDebugString());
            }
            sts.set_error_code(TxOpStatus_Code_CleanNotExist);
            sts.set_error_message(ss.str());
            LOG(WARNING) << ss.str();
            return sts;
        }

        ss << "Tx(" << txid.ShortDebugString() << ") clean on " << "key: "<< key << " success. "
           << "Find "<< (iter->second->HasLock() ? "lock" : "intent") << " Tx(" << iter->second->Holder().ShortDebugString() << ") value: "
           << (iter->second->HasLock() ? "" : iter->second->IntentValue()->ShortDebugString());
        sts.set_error_code(TxOpStatus_Code_Ok);
        sts.set_error_message(ss.str());
        LOG(INFO) << ss.str();

        iter->second->_holder.Clear();
        iter->second->_intent_value.reset(nullptr);
        iter->second->_has_intent = false;
        iter->second->_has_lock = false;

        if (_blocked_ops.find(key) != _blocked_ops.end()) {
            auto iter = _blocked_ops.find(key);
            for (auto& func : iter->second) {
                bthread_t bid;
                auto* arg = new std::function<void()>(func);
                if (bthread_start_background(&bid, nullptr, CallbackWrapper, arg) != 0) {
                    LOG(ERROR) << "Failed to start callback.";
                }
            }
            iter->second.clear();
        }

        return sts;
    }

    virtual TxOpStatus Commit(const UserKey& key, const TxIdentifier& txid) override {
        std::lock_guard<bthread::Mutex> lck(_latch);

        TxOpStatus sts;
        std::stringstream ss;
        auto iter = _kvs.find(key);
        if (iter == _kvs.end()
            || !iter->second->HasIntent()
            || iter->second->Holder().start_ts() != txid.start_ts()) {
            ss << "Tx(" << txid.ShortDebugString() << ") commit on " << "key: "<< key << " not exist. ";
            if (iter != _kvs.end()) {
                assert(!(iter->second->HasIntent() && iter->second->HasLock()));
                ss << "Find " << (iter->second->HasLock() ? "lock" : "intent") << " Tx(" << iter->second->Holder().ShortDebugString() << ") value: "
                   << (iter->second->HasLock() ? "" : iter->second->IntentValue()->ShortDebugString());
            }
            sts.set_error_code(TxOpStatus_Code_CommitNotExist);
            sts.set_error_message(ss.str());
            LOG(WARNING) << ss.str();
            return sts;
        }

        ss << "Tx(" << txid.ShortDebugString() << ") commit on " << "key: "<< key << " success. "
           << "Find "<< "intent" << " Tx(" << iter->second->Holder().ShortDebugString() << ") value: "
           << iter->second->IntentValue()->ShortDebugString();
        sts.set_error_code(TxOpStatus_Code_Ok);
        sts.set_error_message(ss.str());
        LOG(INFO) << ss.str();

        iter->second->_holder.Clear();
        iter->second->_t2v.insert(std::make_pair(txid.commit_ts(), std::move(iter->second->_intent_value)));
        iter->second->_has_intent = false;
        iter->second->_has_lock = false;

        if (_blocked_ops.find(key) != _blocked_ops.end()) {
            auto iter = _blocked_ops.find(key);
            for (auto& func : iter->second) {
                bthread_t bid;
                auto* arg = new std::function<void()>(func);
                if (bthread_start_background(&bid, nullptr, CallbackWrapper, arg) != 0) {
                    LOG(ERROR) << "Failed to start callback.";
                }
            }
            iter->second.clear();
        }

        return sts;
    }

    virtual TxOpStatus Read(const UserKey& key, Value& v, const TxIdentifier& txid, std::function<void()> callback) override {
        std::lock_guard<bthread::Mutex> lck(_latch);

        TxOpStatus sts;
        std::stringstream ss;
        auto iter = _kvs.find(key);
        if (iter == _kvs.end()) {
            ss << "Tx(" << txid.ShortDebugString() << ") read on " << "key: "<< key << " not exist. ";
            sts.set_error_code(TxOpStatus_Code_ReadNotExist);
            sts.set_error_message(ss.str());
            LOG(INFO) << ss.str();
            return sts;
        }

        if ((iter->second->HasIntent() || iter->second->HasLock())
            && iter->second->Holder().start_ts() == txid.start_ts()) {
            assert(!(iter->second->HasIntent() && iter->second->HasLock()));
            ss << "Tx(" << txid.ShortDebugString() << ") read on " << "key: "<< key << " not exist. "
               << "Find its own "<< (iter->second->HasLock() ? "lock" : "intent") << " Tx(" << iter->second->Holder().ShortDebugString() << ") value: "
               << (iter->second->HasLock() ? "" : iter->second->IntentValue()->ShortDebugString());
            sts.set_error_code(TxOpStatus_Code_ReadNotExist);
            sts.set_error_message(ss.str());
            LOG(ERROR) << ss.str();
            return sts;
        }

        if (iter->second->HasIntent() && iter->second->Holder().start_ts() < txid.start_ts()) {
            assert(!iter->second->HasLock());
            ss << "Tx(" << txid.ShortDebugString() << ") read on " << "key: "<< key << " blocked. "
               << "Find "<< "intent" << " Tx(" << iter->second->Holder().ShortDebugString() << ") value: "
               << iter->second->IntentValue()->ShortDebugString();
            sts.set_error_code(TxOpStatus_Code_ReadBlock);
            sts.set_error_message(ss.str());
            LOG(INFO) << ss.str();
            if (_blocked_ops.find(key) == _blocked_ops.end()) {
                _blocked_ops.insert(std::make_pair(key, std::vector<std::function<void()>>()));
            }
            _blocked_ops[key].push_back(callback);
            return sts;
        }

        auto sv = iter->second->Seek(txid.start_ts());
        if (sv.first <= txid.start_ts()) {
            ss << "Tx(" << txid.ShortDebugString() << ") read on " << "key: "<< key << " success. "
               << "Find " << "ts: " << sv.first << " value: "
               << sv.second->ShortDebugString();
            sts.set_error_code(TxOpStatus_Code_Ok);
            sts.set_error_message(ss.str());
            LOG(INFO) << ss.str();
            v.CopyFrom(*(sv.second));
            return sts;
        }

        ss << "Tx(" << txid.ShortDebugString() << ") read on " << "key: "<< key << " not exist. ";
        sts.set_error_code(TxOpStatus_Code_ReadNotExist);
        sts.set_error_message(ss.str());
        LOG(INFO) << ss.str();
        return sts;
    }

private:
    /*butil::FlatMap<UserKey, std::unique_ptr<MVCCValue>> _kvs;
    butil::FlatMap<UserKey, std::vector<std::function<void()>>> _blocked_ops;*/
    std::unordered_map<UserKey, std::unique_ptr<MVCCValue>> _kvs;
    std::unordered_map<UserKey, std::vector<std::function<void()>>> _blocked_ops;
    bthread::Mutex _latch;
};

class TxIndexImpl : public txindex::TxIndex {
public:
    TxIndexImpl() :
    _kvbs(FLAGS_latch_bucket_num) {
        for (auto &it: _kvbs) {
            it.reset(new KVBucket());
        }
    }
    DISALLOW_COPY_AND_ASSIGN(TxIndexImpl);
    ~TxIndexImpl() = default;

    virtual TxOpStatus WriteLock(const UserKey& key, const TxIdentifier& txid, std::function<void()> callback) override {
        auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
        return _kvbs[bucket_num]->WriteLock(key, txid, callback);
    }

    virtual TxOpStatus WriteIntent(const UserKey& key, const Value& v, const TxIdentifier& txid) override {
        auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
        return _kvbs[bucket_num]->WriteIntent(key, v, txid);
    }

    virtual TxOpStatus Clean(const UserKey& key, const TxIdentifier& txid) override {
        auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
        return _kvbs[bucket_num]->Clean(key, txid);
    }

    virtual TxOpStatus Commit(const UserKey& key, const TxIdentifier& txid) override {
        auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
        return _kvbs[bucket_num]->Commit(key, txid);
    }

    virtual TxOpStatus Read(const UserKey& key, Value& v, const TxIdentifier& txid, std::function<void()> callback) override {
        auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
        return _kvbs[bucket_num]->Read(key, v, txid, callback);
    }

private:
    std::vector<std::shared_ptr<KVBucket>> _kvbs;
};

} // namespace

namespace txindex {
    TxIndex* TxIndex::DefaultTxIndex() {
        return new TxIndexImpl();
    }
} // namespace txindex

} // namespace azino