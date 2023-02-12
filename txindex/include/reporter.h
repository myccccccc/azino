#ifndef AZINO_TXINDEX_INCLUDE_REPORTER_H
#define AZINO_TXINDEX_INCLUDE_REPORTER_H

#include <brpc/channel.h>
#include <bthread/execution_queue.h>
#include <butil/macros.h>

#include <string>

#include "service/tx.pb.h"
#include "service/txplanner/txplanner.pb.h"

DECLARE_bool(enable_dep_reporter);

namespace azino {
namespace txindex {
enum DepType { READWRITE = 1 };

typedef struct Dep {
    std::string key;
    DepType type;
    TxIdentifier t1;
    TxIdentifier t2;
} Dep;

typedef std::vector<Dep> Deps;

class KVRegion;
class DepReporter {
   public:
    DepReporter(KVRegion* region, brpc::Channel* txplaner_channel);
    DISALLOW_COPY_AND_ASSIGN(DepReporter);
    ~DepReporter();

    void AsyncReadWriteReport(const Deps& deps);

   private:
    static int execute(void* args, bthread::TaskIterator<Deps>& iter);

    KVRegion* _region;
    txplanner::RegionService_Stub _stub;
    bthread::ExecutionQueueId<Deps> _deps_queue;
};

}  // namespace txindex
}  // namespace azino
#endif  // AZINO_TXINDEX_INCLUDE_REPORTER_H
