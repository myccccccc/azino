#include <butil/hash.h>
#include <butil/time.h>
#include <gflags/gflags.h>

#include <functional>

#include "depedence.h"
#include "index.h"

DEFINE_int32(latch_bucket_num, 128, "latch buckets number");

#define DO_RW_DEP_REPORT(deps)              \
    if (FLAGS_enable_dep_reporter) {        \
        _deprpt.AsyncReadWriteReport(deps); \
    }

namespace azino {
namespace txindex {

KVRegion::KVRegion(brpc::Channel* storage_channel,
                   brpc::Channel* txplaner_channel)
    : _kvbs(FLAGS_latch_bucket_num),
      _persistor(this, storage_channel, txplaner_channel),
      _deprpt(this, txplaner_channel),
      _metric(this, txplaner_channel) {
    if (FLAGS_enable_persistor) {
        _persistor.Start();
    }
    if (FLAGS_enable_region_metric_report) {
        _metric.Start();
    }
}

KVRegion::~KVRegion() {
    if (FLAGS_enable_persistor) {
        _persistor.Stop();
    }
    if (FLAGS_enable_region_metric_report) {
        _metric.Stop();
    }
}

TxOpStatus KVRegion::WriteLock(const std::string& key, const TxIdentifier& txid,
                               std::function<void()> callback) {
    int64_t start_time = butil::gettimeofday_us();
    Deps deps;
    bool is_lock_update = false;
    auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
    auto sts =
        _kvbs[bucket_num].WriteLock(key, txid, callback, deps, is_lock_update);
    DO_RW_DEP_REPORT(deps);
    if (!is_lock_update) {
        _metric.RecordWrite(sts, start_time);
    }
    return sts;
}

TxOpStatus KVRegion::WriteIntent(const std::string& key, const Value& value,
                                 const TxIdentifier& txid,
                                 std::function<void()> callback) {
    int64_t start_time = butil::gettimeofday_us();
    Deps deps;
    bool is_lock_update = false;
    auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
    auto sts = _kvbs[bucket_num].WriteIntent(key, value, txid, callback, deps,
                                             is_lock_update);
    DO_RW_DEP_REPORT(deps);
    if (!is_lock_update) {
        _metric.RecordWrite(sts, start_time);
    }
    return sts;
}

TxOpStatus KVRegion::Clean(const std::string& key, const TxIdentifier& txid) {
    auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
    return _kvbs[bucket_num].Clean(key, txid);
}

TxOpStatus KVRegion::Commit(const std::string& key, const TxIdentifier& txid) {
    auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
    return _kvbs[bucket_num].Commit(key, txid);
}

TxOpStatus KVRegion::Read(const std::string& key, Value& v,
                          const TxIdentifier& txid,
                          std::function<void()> callback) {
    int64_t start_time = butil::gettimeofday_us();
    Deps deps;
    auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
    auto sts = _kvbs[bucket_num].Read(key, v, txid, callback, deps);
    DO_RW_DEP_REPORT(deps);
    _metric.RecordRead(sts, start_time);
    return sts;
}

}  // namespace txindex
}  // namespace azino