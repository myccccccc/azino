#ifndef AZINO_TXINDEX_INCLUDE_REPORTER_H
#define AZINO_TXINDEX_INCLUDE_REPORTER_H

#include <brpc/channel.h>
#include <butil/macros.h>

#include <string>

#include "service/txplanner/txplanner.pb.h"

namespace azino {
namespace txindex {
enum DepType { READWRITE = 1, WRITEWRITE = 2, WRITEREAD = 3 };

typedef struct Dep {
    DepType type;
    uint64_t ts1;
    uint64_t ts2;
} Dependence;

class DepReporter {
   public:
    DepReporter(const std::string& txplanner_addr);
    DISALLOW_COPY_AND_ASSIGN(DepReporter);
    ~DepReporter() = default;

    void ReadWriteReport(const std::string key, uint64_t ts1, uint64_t ts2);

    void WriteWriteReport(const std::string key, uint64_t ts1, uint64_t ts2);

    void WriteReadReport(const std::string key, uint64_t ts1, uint64_t ts2);

   private:
    std::unique_ptr<txplanner::DependenceService_Stub> _stub;
    brpc::Channel _channel;
};
}  // namespace txindex
}  // namespace azino
#endif  // AZINO_TXINDEX_INCLUDE_REPORTER_H
