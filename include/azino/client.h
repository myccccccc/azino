#include <butil/macros.h>
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>

#include "status.h"
#include "kv.h"
#include "options.h"

#ifndef AZINO_INCLUDE_CLIENT_H
#define AZINO_INCLUDE_CLIENT_H

namespace brpc {
    class Channel;
    class ChannelOptions;
}

namespace azino {
    class TxIdentifier;
    class TxWriteBuffer;

    // not thread safe, and it is not reusable.
    class Transaction {
    public:
        Transaction(const Options& options, const std::string& txplanner_addr);
        DISALLOW_COPY_AND_ASSIGN(Transaction);
        ~Transaction();

        // tx operations
        Status Begin();
        Status Commit();

        // kv operations, fail when tx has not started
        Status Put(const WriteOptions& options, const UserKey& key, const UserValue& value);
        Status Get(const ReadOptions& options, const UserKey& key, UserValue& value);
        Status Delete(const WriteOptions& options, const UserKey& key);
        
    private:

        Status Write(const WriteOptions& options, const UserKey& key, bool is_delete, const UserValue& value = "");
        Status PreputAll();
        Status CommitAll();
        Status AbortAll();
        std::unique_ptr<Options> _options;
        std::unique_ptr<brpc::ChannelOptions> _channel_options;
        std::unique_ptr<brpc::Channel> _txplanner;
        std::unique_ptr<brpc::Channel> _storage;
        std::vector<std::unique_ptr<brpc::Channel>> _txindexs;
        std::unique_ptr<TxIdentifier> _txid;
        std::unique_ptr<TxWriteBuffer> _txwritebuffer;
    };


}

#endif // AZINO_INCLUDE_CLIENT_H
