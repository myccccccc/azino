#ifndef AZINO_TXINDEX_INCLUDE_PERSISTOR_H
#define AZINO_TXINDEX_INCLUDE_PERSISTOR_H

#include <brpc/channel.h>
#include <butil/macros.h>
#include <butil/time.h>
#include <gflags/gflags.h>

#include <memory>

#include "azino/background_task.h"
#include "bthread/bthread.h"
#include "bthread/mutex.h"
#include "mvccvalue.h"
#include "service/storage/storage.pb.h"
#include "service/txplanner/txplanner.pb.h"

DECLARE_bool(enable_persistor);

namespace azino {
namespace txindex {
class KVRegion;

struct DataToPersist {
    DataToPersist(const std::string& k, MultiVersionValue::const_iterator begin,
                  MultiVersionValue::const_iterator end)
        : key(k), t2vs(begin, end) {}
    std::string key;
    MultiVersionValue t2vs;
};

class Persistor : public azino::BackgroundTask {
   public:
    Persistor(KVRegion* region, brpc::Channel* storage_channel,
              brpc::Channel* txplaner_channel);
    DISALLOW_COPY_AND_ASSIGN(Persistor);
    ~Persistor() = default;

   private:
    void persist();
    void get_min_ats();

    static void* execute(void* args);

    KVRegion* _region;
    storage::StorageService_Stub _storage_stub;
    txplanner::RegionService_Stub _txplanner_stub;
    size_t _last_persist_bucket_num;
    int64_t _last_get_min_ats_time;
    uint64_t _min_ats;
};

}  // namespace txindex
}  // namespace azino

#endif  // AZINO_TXINDEX_INCLUDE_PERSISTOR_H
