#include <bthread/mutex.h>
#include <butil/hash.h>
#include <gflags/gflags.h>

#include <functional>
#include <unordered_map>

#include "depedence.h"
#include "index.h"

DECLARE_bool(enable_dep_reporter);

#define CHECK_WRITE_TOO_LATE(type)                                       \
    do {                                                                 \
        auto iter = mv.LargestTSValue();                                 \
        if (iter != mv.MVV().end() &&                                    \
            iter->first.commit_ts() >= txid.start_ts()) {                \
            ss << "Tx(" << txid.ShortDebugString() << ") write " << type \
               << " on "                                                 \
               << "key: " << key << " too late. "                        \
               << "Find "                                                \
               << "largest version: " << iter->first.ShortDebugString()  \
               << " value: " << iter->second->ShortDebugString();        \
            sts.set_error_code(TxOpStatus_Code_WriteTooLate);            \
            sts.set_error_message(ss.str());                             \
            return sts;                                                  \
        }                                                                \
    } while (0);

#define CHECK_READ_WRITE_DEP(key, mv, txid, deps)                          \
    do {                                                                   \
        for (auto iter = mv.Readers().begin(); iter != mv.Readers().end(); \
             iter++) {                                                     \
            deps.push_back(txindex::Dep{key, txindex::DepType::READWRITE,  \
                                        iter->second, txid});              \
        }                                                                  \
    } while (0);

namespace azino {
namespace txindex {

TxOpStatus KVBucket::WriteLock(const std::string& key, const TxIdentifier& txid,
                               std::function<void()> callback, Deps& deps,
                               bool& is_lock_update) {
    return Write(MVCCLock::WriteLock, txid, key, Value::default_instance(),
                 callback, deps, is_lock_update);
}

TxOpStatus KVBucket::WriteIntent(const std::string& key, const Value& v,
                                 const TxIdentifier& txid,
                                 std::function<void()> callback, Deps& deps,
                                 bool& is_lock_update) {
    return Write(MVCCLock::WriteIntent, txid, key, v, callback, deps,
                 is_lock_update);
}

TxOpStatus KVBucket::Clean(const std::string& key, const TxIdentifier& txid) {
    std::lock_guard<bthread::Mutex> lck(_latch);

    TxOpStatus sts;
    std::stringstream ss;
    MVCCValue& mv = _kvs[key];

    if (mv.LockType() == MVCCLock::None ||
        mv.LockHolder().start_ts() != txid.start_ts()) {
        ss << "Tx(" << txid.ShortDebugString() << ") clean on "
           << "key: " << key << " not exist. ";
        sts.set_error_code(TxOpStatus_Code_NotExist);
        sts.set_error_message(ss.str());
        LOG(ERROR) << ss.str();
        return sts;
    }

    mv.Clean();
    mv.WakeUpWaiters();

    sts.set_error_code(TxOpStatus_Code_Ok);
    return sts;
}

TxOpStatus KVBucket::Commit(const std::string& key, const TxIdentifier& txid) {
    std::lock_guard<bthread::Mutex> lck(_latch);

    TxOpStatus sts;
    std::stringstream ss;

    MVCCValue& mv = _kvs[key];

    if (mv.LockType() != MVCCLock::WriteIntent ||
        mv.LockHolder().start_ts() != txid.start_ts()) {
        ss << "Tx(" << txid.ShortDebugString() << ") commit on "
           << "key: " << key << " not exist. ";
        sts.set_error_code(TxOpStatus_Code_NotExist);
        sts.set_error_message(ss.str());
        LOG(ERROR) << ss.str();
        return sts;
    }

    mv.Commit(txid);
    mv.WakeUpWaiters();

    sts.set_error_code(TxOpStatus_Code_Ok);
    return sts;
}

TxOpStatus KVBucket::Read(const std::string& key, Value& v,
                          const TxIdentifier& txid,
                          std::function<void()> callback, Deps& deps) {
    std::lock_guard<bthread::Mutex> lck(_latch);

    TxOpStatus sts;
    std::stringstream ss;

    MVCCValue& mv = _kvs[key];

    if (mv.LockType() != MVCCLock::None &&
        mv.LockHolder().start_ts() == txid.start_ts()) {
        ss << "Tx(" << txid.ShortDebugString() << ") read on "
           << "key: " << key << " not exist. "
           << "Find it's own lock type" << mv.LockType()
           << " Tx: " << mv.LockHolder().ShortDebugString();
        sts.set_error_code(TxOpStatus_Code_NotExist);
        sts.set_error_message(ss.str());
        LOG(ERROR) << ss.str();
        return sts;
    }

    if (mv.LockType() == MVCCLock::WriteIntent &&
        mv.LockHolder().start_ts() < txid.start_ts()) {
        ss << "Tx(" << txid.ShortDebugString() << ") read on "
           << "key: " << key << " blocked. "
           << "Find lock type: " << mv.LockType()
           << " Tx: " << mv.LockHolder().ShortDebugString();
        sts.set_error_code(TxOpStatus_Code_ReadBlock);
        sts.set_error_message(ss.str());
        mv.AddWaiter(callback);
        LOG(INFO) << ss.str();
        return sts;
    }

    if (FLAGS_enable_dep_reporter) {
        // uncommitted RW dep
        if (mv.LockType() != MVCCLock::None) {
            deps.push_back(txindex::Dep{key, txindex::DepType::READWRITE, txid,
                                        mv.LockHolder()});
        }

        // committed RW dep
        auto iter = mv.LargestTSValue();
        while (iter != mv.MVV().end() &&
               iter->first.commit_ts() > txid.start_ts()) {
            deps.push_back(txindex::Dep{key, txindex::DepType::READWRITE, txid,
                                        iter->first});
            iter++;
        }

        // add reader
        mv.AddReader(txid);
    }

    auto iter = mv.Seek(txid.start_ts());
    if (iter != mv.MVV().end()) {
        sts.set_error_code(TxOpStatus_Code_Ok);
        v.CopyFrom(*(iter->second));
    } else {
        sts.set_error_code(TxOpStatus_Code_NotExist);
    }
    return sts;
}

int KVBucket::GetPersisting(std::vector<txindex::DataToPersist>& datas,
                            uint64_t min_ats) {
    std::lock_guard<bthread::Mutex> lck(_latch);
    int cnt = 0;
    for (auto& it : _kvs) {
        txindex::DataToPersist d(it.first, it.second.Seek2(min_ats),
                                 it.second.MVV().end());
        if (d.t2vs.size() == 0) {
            continue;
        }
        cnt += d.t2vs.size();
        datas.push_back(d);
    }
    return cnt;
}

int KVBucket::ClearPersisted(const std::vector<txindex::DataToPersist>& datas,
                             RegionMetric* regionMetric) {
    std::lock_guard<bthread::Mutex> lck(_latch);

    int cnt = 0;
    for (const auto& it : datas) {
        if (_kvs.find(it.key) == _kvs.end()) {
            LOG(ERROR) << "UserKey: " << it.key
                       << " clear persist error due to no key in _kvs.";
            goto out;
        }

        auto& mv = _kvs[it.key];
        auto n = mv.Truncate(it.t2vs.begin()->first);
        if (it.t2vs.size() != n) {
            LOG(ERROR)
                << "UserKey: " << it.key
                << " clear persist error due to truncate number not match.";
            goto out;
        }

        cnt += n;

        if (mv.Size() == 0 && mv.LockType() == MVCCLock::None &&
            mv.Readers().empty()) {
            _kvs.erase(it.key);
            if (regionMetric) {
                regionMetric->GCkm(it.key);
            }
        }
    }

out:
    return cnt;
}

TxOpStatus KVBucket::Write(MVCCLock lock_type, const TxIdentifier& txid,
                           const std::string& key, const Value& v,
                           std::function<void()> callback, Deps& deps,
                           bool& is_lock_update) {
    std::lock_guard<bthread::Mutex> lck(_latch);
    TxOpStatus sts;
    std::stringstream ss;
    MVCCValue& mv = _kvs[key];

    CHECK_WRITE_TOO_LATE(lock_type)

    if (mv.LockType() != MVCCLock::None) {
        ss << "Tx(" << txid.ShortDebugString() << ") write " << lock_type
           << " on "
           << "key: " << key << " find lock type: " << mv.LockType() << " Tx("
           << mv.LockHolder().ShortDebugString() << ")";
        if (mv.LockHolder().start_ts() < txid.start_ts()) {
            mv.AddWaiter(callback);
            ss << " blocked";
            sts.set_error_code(TxOpStatus_Code_WriteBlock);
            sts.set_error_message(ss.str());
            LOG(INFO) << ss.str();
            return sts;
        } else if (mv.LockHolder().start_ts() > txid.start_ts()) {
            ss << " conflict";
            sts.set_error_code(TxOpStatus_Code_WriteConflicts);
            sts.set_error_message(ss.str());
            LOG(INFO) << ss.str();
            return sts;
        } else if (mv.LockType() == lock_type) {
            ss << " repeated";
            sts.set_error_code(TxOpStatus_Code_Ok);
            sts.set_error_message(ss.str());
            LOG(WARNING) << ss.str();
            return sts;
        }

        // change write_lock to write_intent
        is_lock_update = true;
        // go down
    }

    if (FLAGS_enable_dep_reporter) {
        CHECK_READ_WRITE_DEP(key, mv, txid, deps)
    }

    switch (lock_type) {
        case MVCCLock::WriteIntent:
            mv.Prewrite(v, txid);
            break;
        case MVCCLock::WriteLock:
            mv.Lock(txid);
            break;
        default:
            assert(0);
    }

    sts.set_error_code(TxOpStatus_Code_Ok);
    return sts;
}
}  // namespace txindex
}  // namespace azino
