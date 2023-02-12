#include "persist.h"

#include <gflags/gflags.h>

#include "index.h"

DEFINE_bool(enable_persistor, true,
            "enable persistor to persist data to storage server");
static bvar::GFlag gflag_enable_persistor("enable_persistor");
DEFINE_int32(persist_period_ms, 100, "persist period time");
static bvar::GFlag gflag_persist_period_ms("persist_period_ms");
DEFINE_int32(getminats_period_s, 2, "get min_ats period time");
static bvar::GFlag gflag_getminats_period_s("getminats_period_s");

namespace azino {
namespace txindex {

RegionPersist::RegionPersist(KVRegion *region, brpc::Channel *storage_channel,
                             brpc::Channel *txplaner_channel)
    : _region(region),
      _storage_stub(storage_channel),
      _txplanner_stub(txplaner_channel),
      _last_persist_bucket_num(0),
      _last_get_min_ats_time(0),
      _min_ats(0) {
    fn = RegionPersist::execute;
}

void *RegionPersist::execute(void *args) {
    auto p = reinterpret_cast<RegionPersist *>(args);
    while (true) {
        bthread_usleep(FLAGS_persist_period_ms * 1000);
        {
            std::lock_guard<bthread::Mutex> lck(p->_mutex);
            if (p->_stopped) {
                break;
            }
        }
        p->get_min_ats();
        p->persist();
    }
    return nullptr;
}

void RegionPersist::persist() {
    brpc::Controller cntl;
    azino::storage::BatchStoreRequest req;
    azino::storage::BatchStoreResponse resp;
    std::vector<DataToPersist> datas;

    size_t persist_bucket_num =
        (_last_persist_bucket_num + 1) % _region->KVBuckets().size();
    auto cnt =
        _region->KVBuckets()[persist_bucket_num].GetPersisting(datas, _min_ats);
    if (cnt == 0) {
        goto out;
    }

    LOG(INFO) << "get data to persist, region:" << _region->Describe()
              << " bucket:" << persist_bucket_num
              << " persist key num:" << datas.size()
              << " persist value num:" << cnt << " min_ats:" << _min_ats;

    for (auto &kv : datas) {
        for (auto &tv : kv.t2vs) {
            azino::storage::StoreData *d = req.add_datas();
            d->set_key(kv.key);
            d->set_ts(tv.first.commit_ts());
            // req take over the "value *" and will free the memory later
            d->set_allocated_value(new Value(*tv.second));
        }
    }

    _storage_stub.BatchStore(&cntl, &req, &resp, NULL);

    if (cntl.Failed()) {
        LOG(WARNING) << "Controller failed error code: " << cntl.ErrorCode()
                     << " error text: " << cntl.ErrorText();
    } else if (resp.status().error_code() != storage::StorageStatus_Code_Ok) {
        LOG(ERROR) << "Fail to batch store mvcc data, error code: "
                   << resp.status().error_code()
                   << " error msg: " << resp.status().error_message();
    } else {
        _region->KVBuckets()[persist_bucket_num].ClearPersisted(datas);
    }

out:
    _last_persist_bucket_num = persist_bucket_num;
    return;
}

void RegionPersist::get_min_ats() {
    brpc::Controller cntl;
    azino::txplanner::GetMinATSRequest req;
    azino::txplanner::GetMinATSResponse resp;
    if (butil::gettimeofday_s() <
        _last_get_min_ats_time + FLAGS_getminats_period_s) {
        return;
    }

    _txplanner_stub.GetMinATS(&cntl, &req, &resp, NULL);
    if (cntl.Failed()) {
        LOG(WARNING) << "Controller failed error code: " << cntl.ErrorCode()
                     << " error text: " << cntl.ErrorText();
        return;
    }

    _min_ats = resp.min_ats();
    _last_get_min_ats_time = butil::gettimeofday_s();

    //    LOG(INFO) << "get min ats region:"
    //              << " min_ats:" << _min_ats;
}
}  // namespace txindex
}  // namespace azino
