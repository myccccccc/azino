#include <butil/macros.h>

#include <memory>
#include <string>
#include <unordered_map>
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

// not thread safe, and it is not reusable.
class Transaction {
   public:
    Transaction(const Options& options, const std::string& txplanner_addr);
    DISALLOW_COPY_AND_ASSIGN(Transaction);
    ~Transaction();

    // tx operations
    Status Begin();
    Status Commit();
    Status Abort(Status reason = Status::Ok());

    // kv operations, fail when tx has not started
    Status Put(const WriteOptions& options, const UserKey& key,
               const UserValue& value);
    Status Get(const ReadOptions& options, const UserKey& key,
               UserValue& value);
    Status Delete(const WriteOptions& options, const UserKey& key);

   private:
    Status Write(const WriteOptions& options, const UserKey& key,
                 bool is_delete, const UserValue& value = "");
    Status PreputAll();
    Status CommitAll();
    Status AbortAll();
    ChannelPtr& Route(const std::string& key);
    Options _options;
    ChannelOptionsPtr _channel_options;
    ChannelPtr _txplanner;
    ChannelPtr _storage;
    ChannelTable _channel_table;
    std::map<Range, ChannelPtr, RangeComparator> _txindexs;
    TxIdentifierPtr _txid;
    TxWriteBufferPtr _txwritebuffer;
};

}  // namespace azino

#endif  // AZINO_INCLUDE_CLIENT_H
