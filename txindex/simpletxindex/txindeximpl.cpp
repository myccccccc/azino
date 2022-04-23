#include <bthread/mutex.h>
#include <gflags/gflags.h>
#include <butil/hash.h>
#include <butil/containers/flat_map.h>
#include <functional>
#include <memory>
#include <unordered_map>
#include <bthread/bthread.h>
#include "persistor.h"

#include "index.h"

DEFINE_int32(latch_bucket_num, 1024, "latch buckets number");
DEFINE_bool(enable_persistor, false, "If enable persistor to persist data to storage server.");

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
    // Finds committed values whose timestamp is smaller or equal than "ts"
    std::pair<TimeStamp, Value*> Seek(TimeStamp ts) {
        auto iter = _t2v.lower_bound(ts);
        if (iter == _t2v.end()) {
            return std::make_pair(MAX_TIMESTAMP, nullptr);
        }
        return std::make_pair(iter->first, iter->second.get());
    }

    // Truncate committed values whose timestamp is smaller or equal than "ts", return the number of values truncated
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
    std::unique_ptr<Value> _intent_value;
    TxIdentifier _holder;
    txindex::MultiVersionValue _t2v;
};

class KVBucket : public txindex::TxIndex {
public:
    KVBucket() = default;
    DISALLOW_COPY_AND_ASSIGN(KVBucket);
    ~KVBucket() = default;

    virtual TxOpStatus WriteLock(const std::string& key, const TxIdentifier& txid, std::function<void()> callback) override {
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

    virtual TxOpStatus WriteIntent(const std::string& key, const Value& v, const TxIdentifier& txid) override {
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

    virtual TxOpStatus Clean(const std::string& key, const TxIdentifier& txid) override {
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

    virtual TxOpStatus Commit(const std::string& key, const TxIdentifier& txid) override {
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

    virtual TxOpStatus Read(const std::string& key, Value& v, const TxIdentifier& txid, std::function<void()> callback) override {
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

    virtual TxOpStatus GetPersisting(std::vector<txindex::DataToPersist> &datas) override {
        std::lock_guard<bthread::Mutex> lck(_latch);

        TxOpStatus sts;
        std::stringstream ss;
        unsigned long cnt = 0;
        for (auto &it: _kvs) {
            if (it.second->_t2v.empty()) {
                continue;
            }
            txindex::DataToPersist d;
            d.key = it.first;
            d.t2vs = it.second->_t2v;
            cnt += it.second->_t2v.size();
            datas.push_back(d);
        }
        if (cnt == 0) {
            sts.set_error_code(TxOpStatus_Code_NoneToPersist);
            ss << "Get data to persist fail. "
               << "Persist key num: " << datas.size()
               << "Persist value num: " << cnt;
            sts.set_error_message(ss.str());
            LOG(INFO) << ss.str();
        } else {
            sts.set_error_code(TxOpStatus_Code_Ok);
            ss << "Get data to persist success. "
               << "Persist key num: " << datas.size()
               << "Persist value num: " << cnt;
            sts.set_error_message(ss.str());
            LOG(INFO) << ss.str();
        }
        return sts;
    }

    virtual TxOpStatus ClearPersisted(const std::vector<txindex::DataToPersist> &datas) override {
        std::lock_guard<bthread::Mutex> lck(_latch);

        TxOpStatus sts;
        std::stringstream ss;
        unsigned long cnt = 0;
        for (auto &it: datas) {
            assert(!it.t2vs.empty());
            if (_kvs.find(it.key) == _kvs.end()) {
                sts.set_error_code(TxOpStatus_Code_ClearRepeat);
                ss << "UserKey: " << it.key
                   << "repeat clear due to no key in _kvs.";
                break;
            }
            auto n = _kvs[it.key]->Truncate(it.t2vs.begin()->first);
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
    std::unordered_map<std::string, std::unique_ptr<MVCCValue>> _kvs;
    std::unordered_map<std::string, std::vector<std::function<void()>>> _blocked_ops;
    bthread::Mutex _latch;
};

class TxIndexImpl : public txindex::TxIndex {
public:
    TxIndexImpl(const std::string& storage_addr) :
    _kvbs(FLAGS_latch_bucket_num),
    _persistor(this, storage_addr),
    _last_persist_bucket_num(0) {
        for (auto &it: _kvbs) {
            it.reset(new KVBucket());
        }
        if(FLAGS_enable_persistor){
            _persistor.Start();
        }
    }
    DISALLOW_COPY_AND_ASSIGN(TxIndexImpl);
    ~TxIndexImpl() {
        if(FLAGS_enable_persistor){
            _persistor.Stop();
        }
    }

    virtual TxOpStatus WriteLock(const std::string& key, const TxIdentifier& txid, std::function<void()> callback) override {
        auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
        return _kvbs[bucket_num]->WriteLock(key, txid, callback);
    }

    virtual TxOpStatus WriteIntent(const std::string& key, const Value& v, const TxIdentifier& txid) override {
        auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
        return _kvbs[bucket_num]->WriteIntent(key, v, txid);
    }

    virtual TxOpStatus Clean(const std::string& key, const TxIdentifier& txid) override {
        auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
        return _kvbs[bucket_num]->Clean(key, txid);
    }

    virtual TxOpStatus Commit(const std::string& key, const TxIdentifier& txid) override {
        auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
        return _kvbs[bucket_num]->Commit(key, txid);
    }

    virtual TxOpStatus Read(const std::string& key, Value& v, const TxIdentifier& txid, std::function<void()> callback) override {
        auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
        return _kvbs[bucket_num]->Read(key, v, txid, callback);
    }

    virtual TxOpStatus GetPersisting(std::vector<txindex::DataToPersist> &datas) override {
        TxOpStatus res;
        for (int i = 0; i < FLAGS_latch_bucket_num; i++) {
            _last_persist_bucket_num++;
            res = _kvbs[_last_persist_bucket_num % FLAGS_latch_bucket_num]->GetPersisting(datas);
            if (res.error_code() == TxOpStatus_Code_Ok) {
                assert(!datas.empty());
                return res;
            }
        }
        return res;
    }

    virtual TxOpStatus ClearPersisted(const std::vector<txindex::DataToPersist> &datas) override {
        return _kvbs[_last_persist_bucket_num % FLAGS_latch_bucket_num]->ClearPersisted(datas);
    }
private:
    std::vector<std::unique_ptr<KVBucket>> _kvbs;
    txindex::Persistor _persistor;
    uint32_t _last_persist_bucket_num;
};

} // namespace

namespace txindex {
    TxIndex* TxIndex::DefaultTxIndex(const std::string& storage_addr) {
        return new TxIndexImpl(storage_addr);
    }
} // namespace txindex

} // namespace azino