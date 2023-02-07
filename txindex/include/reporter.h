#ifndef AZINO_TXINDEX_INCLUDE_REPORTER_H
#define AZINO_TXINDEX_INCLUDE_REPORTER_H

#include <brpc/channel.h>
#include <butil/macros.h>

#include <string>

#include "service/tx.pb.h"
#include "service/txplanner/txplanner.pb.h"

namespace azino {
namespace txindex {
enum DepType { READWRITE = 1 };

typedef struct Dep {
    DepType type;
    TxIdentifier t1;
    TxIdentifier t2;
} Dependence;

class DepReporter {
   public:
    DepReporter(const std::string& txplanner_addr);
    DISALLOW_COPY_AND_ASSIGN(DepReporter);
    ~DepReporter() = default;

    void ReadWriteReport(const std::string key, const std::vector<Dep>& deps);

   private:
    std::unique_ptr<txplanner::DependenceService_Stub> _stub;
    brpc::Channel _channel;
};
}  // namespace txindex
}  // namespace azino
#endif  // AZINO_TXINDEX_INCLUDE_REPORTER_H
