//
// Created by os on 4/15/22.
//
#include <butil/macros.h>
#include <gflags/gflags.h>
#include "bthread/bthread.h"
#include "bthread/mutex.h"
#include <memory>
#include <brpc/channel.h>
#include "index.h"
#include "service/storage/storage.pb.h"

#ifndef AZINO_TXINDEX_INCLUDE_PERSISTOR_H
#define AZINO_TXINDEX_INCLUDE_PERSISTOR_H
DEFINE_int32(persist_period, 10000, "persist period time. measurement: millisecond");
DEFINE_string(storage_addr, "0.0.0.0:8000", "Address of storage");
namespace azino {
    namespace txindex {

        class Persistor {
        public:
            Persistor(TxIndex *index, uint32_t bucket_num) : _txindex(index), _bid(0),
                                                             _bucket_num(bucket_num),
                                                             _current_bucket(0) {


                // Initialize the channel, NULL means using default options.

                brpc::ChannelOptions option;
                if (_channel.Init(FLAGS_storage_addr.c_str(), "", &option) != 0) {
                    LOG(ERROR) << "Fail to initialize channel";
                }
                _stub.reset(new storage::StorageService_Stub(&_channel));
            };


            DISALLOW_COPY_AND_ASSIGN(Persistor);

            ~Persistor() = default;

            //Start a new thread and monitor the data. GetPersisting data periodically. Return 0 if success.
            int Start() {
                std::lock_guard<bthread::Mutex> lck(_mutex);
                if (_bid != 0) {
                    return -1;
                }
                return bthread_start_background(&_bid, &BTHREAD_ATTR_NORMAL, excute, this);
            }

            //Stop monitor thread. Need call first before the monitoring data destroy. Return 0 if success.
            int Stop() {

                bthread_t bid;

                {
                    std::lock_guard<bthread::Mutex> lck(_mutex);
                    if (_bid == 0) {
                        return -1;
                    }
                    bid = _bid;
                    _bid = 0;
                }


                auto code = bthread_stop(bid);
                return code;
            }

            void Persist() {
                std::vector<DataToPersist> datas;
                auto status = _txindex->GetPersisting(_current_bucket, datas);
                _current_bucket = (_current_bucket + 1) % _bucket_num;
                if (TxOpStatus_Code_Ok != status.error_code()) {

                } else {
                    LOG(INFO) << "persist num: "<<datas.size();
                    if (datas.size() > 0) {
                    }
                }

            }

        private:
            static void *excute(void *args) {
                auto p = (Persistor *) args;
                while (true) {
                    bthread_usleep(FLAGS_persist_period * 1000);
                    std::lock_guard<bthread::Mutex> lck(p->_mutex);
                    if(p->_bid != bthread_self()){
                        break;
                    }
                    p->Persist();
                }

            }


            std::unique_ptr<storage::StorageService_Stub> _stub;
            brpc::Channel _channel;
            TxIndex *_txindex;
            uint32_t _bucket_num;
            uint32_t _current_bucket;
            bthread::Mutex _mutex;
            bthread_t _bid;
        };


    }
}
#endif //AZINO_TXINDEX_INCLUDE_PERSISTOR_H
