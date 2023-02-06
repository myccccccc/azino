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

DEFINE_int32(latch_bucket_num, 1024, "latch buckets number");
DEFINE_bool(enable_persistor, false,
            "If enable persistor to persist data to storage server.");

namespace azino {
namespace txindex {

TxIndexImpl::TxIndexImpl(const std::string& storage_addr)
    : _kvbs(FLAGS_latch_bucket_num),
      _persistor(this, storage_addr),
      _last_persist_bucket_num(0) {
    if (FLAGS_enable_persistor) {
        _persistor.Start();
    }
}

TxIndexImpl::~TxIndexImpl() {
    if (FLAGS_enable_persistor) {
        _persistor.Stop();
    }
}

TxOpStatus TxIndexImpl::WriteLock(const std::string& key,
                                  const TxIdentifier& txid,
                                  std::function<void()> callback,
                                  std::vector<txindex::Dep>& deps) {
    // todo: use a wrapper, and another hash function
    auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
    return _kvbs[bucket_num].WriteLock(key, txid, callback, deps);
}

TxOpStatus TxIndexImpl::WriteIntent(const std::string& key, const Value& value,
                                    const TxIdentifier& txid,
                                    std::vector<txindex::Dep>& deps) {
    // todo: use a wrapper, and another hash function
    auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
    return _kvbs[bucket_num].WriteIntent(key, value, txid, deps);
}

TxOpStatus TxIndexImpl::Clean(const std::string& key,
                              const TxIdentifier& txid) {
    // todo: use a wrapper, and another hash function
    auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
    return _kvbs[bucket_num].Clean(key, txid);
}

TxOpStatus TxIndexImpl::Commit(const std::string& key,
                               const TxIdentifier& txid) {
    // todo: use a wrapper, and another hash function
    auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
    return _kvbs[bucket_num].Commit(key, txid);
}

TxOpStatus TxIndexImpl::Read(const std::string& key, Value& v,
                             const TxIdentifier& txid,
                             std::function<void()> callback,
                             std::vector<txindex::Dep>& deps) {
    // todo: use a wrapper, and another hash function
    auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
    return _kvbs[bucket_num].Read(key, v, txid, callback, deps);
}

TxOpStatus TxIndexImpl::GetPersisting(
    std::vector<txindex::DataToPersist>& datas) {
    TxOpStatus res;
    for (int i = 0; i < FLAGS_latch_bucket_num; i++) {
        _last_persist_bucket_num++;
        res = _kvbs[_last_persist_bucket_num % FLAGS_latch_bucket_num]
                  .GetPersisting(datas);
    }
    return res;
}

TxOpStatus TxIndexImpl::ClearPersisted(
    const std::vector<txindex::DataToPersist>& datas) {
    return _kvbs[_last_persist_bucket_num % FLAGS_latch_bucket_num]
        .ClearPersisted(datas);
}

TxIndex* TxIndex::DefaultTxIndex(const std::string& storage_addr) {
    return new TxIndexImpl(storage_addr);
}
}  // namespace txindex

}  // namespace azino