#include <bthread/mutex.h>
#include <butil/hash.h>
#include <gflags/gflags.h>

#include <functional>
#include <unordered_map>

#include "depedence.h"
#include "index.h"

DECLARE_bool(enable_dep_reporter);
DEFINE_int32(max_data_to_persist_per_round, 1000,
             "max data to persist per round");
DEFINE_bool(first_commit_wins, false, "first commit wins");

#define LOG_WRITE_ERROR(type)                                                \
    LOG(INFO) << "Tx(" << txid.ShortDebugString() << ") write " << lock_type \
              << " on key: " << key << " find lock type: " << mv.LockType()  \
              << " Tx(" << mv.LockHolder().ShortDebugString() << ") "        \
              << #type;

#define CHECK_WRITE_TOO_LATE(type)                                       \
    do {                                                                 \
        auto iter = mv.LargestTSValue();                                 \
        if (iter != mv.MVV().end() &&                                    \
            iter->first.commit_ts() >= txid.start_ts()) {                \
            LOG(INFO) << "Tx(" << txid.ShortDebugString() << ") write "  \
                      << type << " on key: " << key << " too late. "     \
                      << "Find largest version: "                        \
                      << iter->first.ShortDebugString()                  \
                      << " value: " << iter->second->ShortDebugString(); \
            sts.set_error_code(TxOpStatus_Code_WriteTooLate);            \
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

DEFINE_double(lambda, 0.3, "lambda hyper parameter");
static bvar::GFlag gflag_lambda("lambda");

namespace azino {
namespace txindex {

TxOpStatus KVBucket::WriteLock(const std::string& key, const TxIdentifier& txid,
                               std::function<void()> callback, Deps& deps,
                               bool& is_lock_update, bool& is_pess_key) {
    return Write(MVCCLock::WriteLock, txid, key, Value::default_instance(),
                 callback, deps, is_lock_update, is_pess_key);
}

TxOpStatus KVBucket::WriteIntent(const std::string& key, const Value& v,
                                 const TxIdentifier& txid,
                                 std::function<void()> callback, Deps& deps,
                                 bool& is_lock_update, bool& is_pess_key) {
    return Write(MVCCLock::WriteIntent, txid, key, v, callback, deps,
                 is_lock_update, is_pess_key);
}

TxOpStatus KVBucket::Clean(const std::string& key, const TxIdentifier& txid) {
    std::lock_guard<bthread::Mutex> lck(_latch);

    TxOpStatus sts;
    MVCCValue& mv = _kvs[key].mv;

    if (mv.LockType() == MVCCLock::None ||
        mv.LockHolder().start_ts() != txid.start_ts()) {
        sts.set_error_code(TxOpStatus_Code_NotExist);
        LOG(ERROR) << "Tx(" << txid.ShortDebugString() << ") clean on "
                   << "key: " << key << " not exist. ";
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

    MVCCValue& mv = _kvs[key].mv;

    if (mv.LockType() != MVCCLock::WriteIntent ||
        mv.LockHolder().start_ts() != txid.start_ts()) {
        sts.set_error_code(TxOpStatus_Code_NotExist);
        LOG(ERROR) << "Tx(" << txid.ShortDebugString() << ") commit on "
                   << "key: " << key << " not exist. ";
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

    MVCCValue& mv = _kvs[key].mv;

    if (mv.LockType() != MVCCLock::None &&
        mv.LockHolder().start_ts() == txid.start_ts()) {
        sts.set_error_code(TxOpStatus_Code_NotExist);
        LOG(ERROR) << "Tx(" << txid.ShortDebugString() << ") read on "
                   << "key: " << key << " not exist. "
                   << "Find it's own lock type" << mv.LockType()
                   << " Tx: " << mv.LockHolder().ShortDebugString();
        return sts;
    }

    if (mv.LockType() == MVCCLock::WriteIntent &&
        mv.LockHolder().start_ts() < txid.start_ts()) {
        sts.set_error_code(TxOpStatus_Code_ReadBlock);
        mv.AddWaiter(callback);
        LOG(INFO) << "Tx(" << txid.ShortDebugString() << ") read on "
                  << "key: " << key << " blocked. "
                  << "Find lock type: " << mv.LockType()
                  << " Tx: " << mv.LockHolder().ShortDebugString();
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

void KVBucket::gc_mv(RegionMetric* regionMetric) {
    std::lock_guard<bthread::Mutex> lck(_latch);

    std::vector<std::string> gc_keys;
    for (auto& it : _kvs) {
        auto& mv = it.second.mv;
        if (mv.Size() == 0 && mv.LockType() == MVCCLock::None &&
            mv.Readers().empty()) {
            gc_keys.push_back(it.first);
        }
    }

    if (!gc_keys.empty()) {
        LOG(NOTICE) << "GC MV Size:" << gc_keys.size();
    }
    for (auto& key : gc_keys) {
        _kvs.erase(key);
    }
}

int KVBucket::GetPersisting(std::vector<txindex::DataToPersist>& datas,
                            uint64_t min_ats) {
    std::lock_guard<bthread::Mutex> lck(_latch);
    int cnt = 0;
    for (auto& it : _kvs) {
        if (cnt > FLAGS_max_data_to_persist_per_round) {
            break;
        }
        auto& mv = it.second.mv;
        txindex::DataToPersist d(it.first, mv.Seek2(min_ats), mv.MVV().end());
        if (d.t2vs.size() == 0) {
            continue;
        }
        cnt += d.t2vs.size();
        datas.push_back(d);
    }
    return cnt;
}

int KVBucket::ClearPersisted(const std::vector<txindex::DataToPersist>& datas) {
    std::lock_guard<bthread::Mutex> lck(_latch);

    int cnt = 0;
    for (const auto& it : datas) {
        if (_kvs.find(it.key) == _kvs.end()) {
            LOG(ERROR) << "UserKey: " << it.key
                       << " clear persist error due to no key in _kvs.";
            goto out;
        }

        auto& mv = _kvs[it.key].mv;
        auto n = mv.Truncate(it.t2vs.begin()->first);
        if (it.t2vs.size() != n) {
            LOG(ERROR)
                << "UserKey: " << it.key
                << " clear persist error due to truncate number not match.";
            goto out;
        }

        cnt += n;
    }

out:
    return cnt;
}

TxOpStatus KVBucket::Write(MVCCLock lock_type, const TxIdentifier& txid,
                           const std::string& key, const Value& v,
                           std::function<void()> callback, Deps& deps,
                           bool& is_lock_update, bool& is_pess_key) {
    std::lock_guard<bthread::Mutex> lck(_latch);
    ValueAndMetric& vm = _kvs[key];
    TxOpStatus sts =
        write(vm, lock_type, txid, key, v, callback, deps, is_lock_update);
    if (!is_lock_update) {
        KeyMetric& km = vm.km;
        km.RecordWrite();
        if (sts.error_code() != TxOpStatus_Code_Ok) {
            km.RecordWriteError();
        }
        if (km.PessimismDegree() > FLAGS_lambda) {
            is_pess_key = true;
        }
    }
    return sts;
}

TxOpStatus KVBucket::write(ValueAndMetric& vm, MVCCLock lock_type,
                           const TxIdentifier& txid, const std::string& key,
                           const Value& v, std::function<void()> callback,
                           Deps& deps, bool& is_lock_update) {
    TxOpStatus sts;
    MVCCValue& mv = vm.mv;

    if (mv.LockType() == MVCCLock::WriteLock &&
        lock_type == MVCCLock::WriteIntent &&
        mv.LockHolder().start_ts() == txid.start_ts()) {
        is_lock_update = true;
        goto out;
    }

    if (FLAGS_first_commit_wins || lock_type == MVCCLock::WriteIntent) {
        CHECK_WRITE_TOO_LATE(lock_type)
    }

    if (mv.LockType() != MVCCLock::None) {
        if (mv.LockHolder().start_ts() < txid.start_ts()) {
            LOG_WRITE_ERROR(block)
            mv.AddWaiter(callback);
            sts.set_error_code(TxOpStatus_Code_WriteBlock);
            return sts;
        } else if (mv.LockHolder().start_ts() > txid.start_ts()) {
            LOG_WRITE_ERROR(conflict)
            sts.set_error_code(TxOpStatus_Code_WriteConflicts);
            return sts;
        } else {
            LOG_WRITE_ERROR(repeated)
            sts.set_error_code(TxOpStatus_Code_Ok);
            return sts;
        }
    }

out:
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
