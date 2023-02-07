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
#include "reporter.h"

// todo: need to consider the ltv in the storage
#define CHECK_WRITE_TOO_LATE(type)                                        \
    do {                                                                  \
        auto iter = mv.LargestTSValue();                                  \
        if (iter != mv.MVV().end() &&                                     \
            iter->first.commit_ts() >= txid.start_ts()) {                 \
            ss << "Tx(" << txid.ShortDebugString() << ") write " << #type \
               << " on "                                                  \
               << "key: " << key << " too late. "                         \
               << "Find "                                                 \
               << "largest version: " << iter->first.ShortDebugString()   \
               << " value: " << iter->second->ShortDebugString();         \
            sts.set_error_code(TxOpStatus_Code_WriteTooLate);             \
            sts.set_error_message(ss.str());                              \
            return sts;                                                   \
        }                                                                 \
    } while (0);

#define CHECK_READ_WRITE_DEP(mv, txid, deps)                               \
    do {                                                                   \
        for (auto iter = mv.Readers().begin(); iter != mv.Readers().end(); \
             iter++) {                                                     \
            deps.push_back(txindex::Dep{txindex::DepType::READWRITE,       \
                                        iter->second, txid});              \
        }                                                                  \
    } while (0);

namespace azino {
namespace txindex {

TxOpStatus KVBucket::WriteLock(const std::string& key, const TxIdentifier& txid,
                               std::function<void()> callback,
                               std::vector<txindex::Dep>& deps) {
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
               << (mv.HasLock() ? "" : mv.IntentValue()->ShortDebugString());
            sts.set_error_code(TxOpStatus_Code_WriteBlock);
            sts.set_error_message(ss.str());
            mv.AddWaiter(callback);
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

    CHECK_READ_WRITE_DEP(mv, txid, deps)

    mv.Lock(txid);
    sts.set_error_code(TxOpStatus_Code_Ok);
    sts.set_error_message(ss.str());
    return sts;
}

TxOpStatus KVBucket::WriteIntent(const std::string& key, const Value& v,
                                 const TxIdentifier& txid,
                                 std::vector<txindex::Dep>& deps) {
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
               << (mv.HasLock() ? "" : mv.IntentValue()->ShortDebugString());
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

    CHECK_READ_WRITE_DEP(mv, txid, deps)

    mv.Prewrite(v, txid);
    sts.set_error_code(TxOpStatus_Code_Ok);
    sts.set_error_message(ss.str());
    return sts;
}

TxOpStatus KVBucket::Clean(const std::string& key, const TxIdentifier& txid) {
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
               << (mv.HasLock() ? "" : mv.IntentValue()->ShortDebugString());
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

    mv.Clean();

    mv.WakeUpWaiters();

    return sts;
}

TxOpStatus KVBucket::Commit(const std::string& key, const TxIdentifier& txid) {
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
               << (mv.HasLock() ? "" : mv.IntentValue()->ShortDebugString());
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

    mv.Commit(txid);

    mv.WakeUpWaiters();

    return sts;
}

TxOpStatus KVBucket::Read(const std::string& key, Value& v,
                          const TxIdentifier& txid,
                          std::function<void()> callback,
                          std::vector<txindex::Dep>& deps) {
    std::lock_guard<bthread::Mutex> lck(_latch);

    TxOpStatus sts;
    std::stringstream ss;

    MVCCValue& mv = _kvs[key];

    if ((mv.HasIntent() || mv.HasLock()) &&
        mv.Holder().start_ts() == txid.start_ts()) {
        assert(!(mv.HasIntent() && mv.HasLock()));

        ss << "Tx(" << txid.ShortDebugString() << ") read on "
           << "key: " << key << " not exist. "
           << "Find it's own " << (mv.HasLock() ? "lock" : "intent") << " Tx("
           << mv.Holder().ShortDebugString() << ") value: "
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
        mv.AddWaiter(callback);
        return sts;
    }

    // uncommitted RW dep
    if (mv.HasIntent() || mv.HasLock()) {
        deps.push_back(
            txindex::Dep{txindex::DepType::READWRITE, txid, mv.Holder()});
    }

    // committed RW dep
    auto iter = mv.LargestTSValue();
    while (iter != mv.MVV().end() &&
           iter->first.commit_ts() > txid.start_ts()) {
        deps.push_back(
            txindex::Dep{txindex::DepType::READWRITE, txid, iter->first});
        iter++;
    }

    // add reader
    mv.AddReader(txid);

    iter = mv.Seek(txid.start_ts());
    if (iter != mv.MVV().end()) {
        ss << "Tx(" << txid.ShortDebugString() << ") read on "
           << "key: " << key << " success. "
           << "Find "
           << "version: " << iter->first.ShortDebugString()
           << " value: " << iter->second->ShortDebugString();
        sts.set_error_code(TxOpStatus_Code_Ok);
        sts.set_error_message(ss.str());
        v.CopyFrom(*(iter->second));
    } else {
        ss << "Tx(" << txid.ShortDebugString() << ") read on "
           << "key: " << key << " not exist. ";
        sts.set_error_code(TxOpStatus_Code_ReadNotExist);
        sts.set_error_message(ss.str());
    }
    return sts;
}

TxOpStatus KVBucket::GetPersisting(std::vector<txindex::DataToPersist>& datas) {
    std::lock_guard<bthread::Mutex> lck(_latch);

    TxOpStatus sts;
    std::stringstream ss;
    unsigned long cnt = 0;
    for (auto& it : _kvs) {
        if (it.second.Size() == 0) {
            continue;
        }
        txindex::DataToPersist d;
        d.key = it.first;
        d.t2vs = it.second.MVV();
        cnt += it.second.MVV().size();
        datas.push_back(d);
    }
    if (cnt == 0) {
        sts.set_error_code(TxOpStatus_Code_NoneToPersist);
    } else {
        sts.set_error_code(TxOpStatus_Code_Ok);
    }

    ss << "Get data to persist. "
       << "Persist key num: " << datas.size() << "Persist value num: " << cnt;
    sts.set_error_message(ss.str());
    LOG(INFO) << ss.str();
    return sts;
}

TxOpStatus KVBucket::ClearPersisted(
    const std::vector<txindex::DataToPersist>& datas) {
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
}  // namespace txindex
}  // namespace azino