#include <butil/hash.h>
#include <gflags/gflags.h>

#include <functional>

#include "index.h"
#include "reporter.h"

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
      _deprpt(txplaner_channel) {
    if (FLAGS_enable_persistor) {
        _persistor.Start();
    }
}

KVRegion::~KVRegion() {
    if (FLAGS_enable_persistor) {
        _persistor.Stop();
    }
}

TxOpStatus KVRegion::WriteLock(const std::string& key, const TxIdentifier& txid,
                               std::function<void()> callback, Deps& deps) {
    auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
    auto sts = _kvbs[bucket_num].WriteLock(key, txid, callback, deps);
    DO_RW_DEP_REPORT(deps);
    return sts;
}

TxOpStatus KVRegion::WriteIntent(const std::string& key, const Value& value,
                                 const TxIdentifier& txid, Deps& deps) {
    auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
    auto sts = _kvbs[bucket_num].WriteIntent(key, value, txid, deps);
    DO_RW_DEP_REPORT(deps);
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
                          std::function<void()> callback, Deps& deps) {
    auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
    auto sts = _kvbs[bucket_num].Read(key, v, txid, callback, deps);
    DO_RW_DEP_REPORT(deps);
    return sts;
}

}  // namespace txindex
}  // namespace azino