#include <brpc/channel.h>
#include <butil/macros.h>
#include <gflags/gflags.h>

#include <memory>

#include "azino/background_task.h"
#include "bthread/bthread.h"
#include "bthread/mutex.h"
#include "index.h"
#include "service/storage/storage.pb.h"

#ifndef AZINO_TXINDEX_INCLUDE_PERSISTOR_H
#define AZINO_TXINDEX_INCLUDE_PERSISTOR_H

namespace azino {
namespace txindex {
class TxIndex;
class Persistor : public azino::BackgroundTask {
   public:
    Persistor(TxIndex *index, const std::string &storage_addr);
    DISALLOW_COPY_AND_ASSIGN(Persistor);
    ~Persistor() = default;

   private:
    // Need hold _mutex before call this func.
    // If _txindex want to destruct itself, it needs call Stop() and hold _mutex
    // first, therefore if persist is called, it can make sure to access
    // _txindex's data.
    void persist();

    static void *execute(void *args);

    std::unique_ptr<storage::StorageService_Stub> _stub;
    brpc::Channel _channel;
    TxIndex *_txindex;
};

}  // namespace txindex
}  // namespace azino

#endif  // AZINO_TXINDEX_INCLUDE_PERSISTOR_H
