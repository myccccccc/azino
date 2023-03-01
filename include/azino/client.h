#include <butil/macros.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "kv.h"
#include "options.h"
#include "range.h"
#include "status.h"

#ifndef AZINO_INCLUDE_CLIENT_H
#define AZINO_INCLUDE_CLIENT_H

namespace brpc {
class Channel;
class ChannelOptions;
}  // namespace brpc

namespace azino {
class TxIdentifier;
class TxWriteBuffer;

typedef std::unique_ptr<brpc::ChannelOptions> ChannelOptionsPtr;
typedef std::shared_ptr<brpc::Channel> ChannelPtr;
typedef std::unique_ptr<TxIdentifier> TxIdentifierPtr;
typedef std::unique_ptr<TxWriteBuffer> TxWriteBufferPtr;
typedef std::map<std::string, ChannelPtr> ChannelTable;

typedef struct Region {
    ChannelPtr channel;
    std::unordered_set<std::string> pk;
} Region;

typedef std::map<Range, Region, RangeComparator> PartitionRouteTable;

// not thread safe
class Transaction {
   public:
    Transaction(const Options& options);
    DISALLOW_COPY_AND_ASSIGN(Transaction);
    ~Transaction();

    // tx operations
    Status Begin();
    Status Commit();
    Status Abort(Status reason = Status::Ok());

    // kv operations, fail when tx has not started
    Status Put(WriteOptions options, const UserKey& key,
               const UserValue& value);
    Status Get(ReadOptions options, const UserKey& key, UserValue& value);
    Status Delete(WriteOptions options, const UserKey& key);
    // include left_key, not include right_key
    Status Scan(const UserKey& left_key, const UserKey& right_key,
                std::vector<UserValue>& keys, std::vector<UserValue>& values);

    void Reset();

   private:
    Status Write(WriteOptions options, const UserKey& key, bool is_delete,
                 const UserValue& value = "");
    Status PreputAll();
    Status CommitAll();
    Status AbortAll();
    Region& Route(const std::string& key);
    Options _options;
    ChannelPtr _txplanner;
    ChannelPtr _storage;
    ChannelTable _channel_table;
    PartitionRouteTable _route_table;
    TxIdentifierPtr _txid;
    TxWriteBufferPtr _txwritebuffer;
};

}  // namespace azino

#endif  // AZINO_INCLUDE_CLIENT_H
