#ifndef AZINO_TXINDEX_INCLUDE_PERSISTOR_H
#define AZINO_TXINDEX_INCLUDE_PERSISTOR_H

#include <brpc/channel.h>
#include <butil/macros.h>
#include <gflags/gflags.h>

#include <memory>

#include "azino/background_task.h"
#include "bthread/bthread.h"
#include "bthread/mutex.h"
#include "mvccvalue.h"
#include "service/storage/storage.pb.h"

DECLARE_bool(enable_persistor);

namespace azino {
namespace txindex {
class KVRegion;

struct DataToPersist {
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

    static void* execute(void* args);

    KVRegion* _region;
    storage::StorageService_Stub _storage_stub;
    size_t _last_persist_bucket_num;
};

}  // namespace txindex
}  // namespace azino

#endif  // AZINO_TXINDEX_INCLUDE_PERSISTOR_H
