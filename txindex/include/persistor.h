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
DECLARE_int32(persist_period);
DECLARE_string(storage_addr);
DECLARE_bool(enable_persistor);
namespace azino {
namespace txindex {

    class Persistor {
    public:
        Persistor(TxIndex *index, uint32_t bucket_num) : _txindex(index),
                                                         _bucket_num(bucket_num),
                                                         _current_bucket(0),_bid(-1) {


            // Initialize the channel, NULL means using default options.

            brpc::ChannelOptions option;
            if (_channel.Init(FLAGS_storage_addr.c_str(),  &option) != 0) {
                LOG(ERROR) << "Fail to initialize channel";
            }
            _stub.reset(new storage::StorageService_Stub(&_channel));
        };


        DISALLOW_COPY_AND_ASSIGN(Persistor);

        ~Persistor() = default;

        //Start a new thread and monitor the data. GetPersisting data periodically. Return 0 if success.
        int Start() {
            std::lock_guard<bthread::Mutex> lck(_mutex);
            if (!is_stopped()) {
                return -1;
            }
            return bthread_start_background(&_bid, NULL , execute, this);
        }

        //Stop monitor thread. Need call first before the monitoring data destroy. Return 0 if success.
        int Stop() {

            bthread_t bid;

            {
                // reset _bid, if the bthread wake up and found a different _bid, it will exit.
                std::lock_guard<bthread::Mutex> lck(_mutex);
                if (is_stopped()) {
                    return -1;
                }
                bid = _bid;
                _bid = -1;
            }

            bthread_stop(bid);
            return bthread_join(bid, NULL);
        }


    private:

        //Need hold _mutex before call this func.
        //If _txindex want to destruct itself, it needs call Stop() and hold _mutex first,
        //therefore if persist is called, it can make sure to access _txindex's data.
        void persist() {

            std::vector<DataToPersist> datas;
            // iterate bucket_num times to find a non-empty bucket. If not found, sleep.
            for (int i = 0; i < _bucket_num; i++) {
                auto current_bucket = _current_bucket;
                auto status = _txindex->GetPersisting(current_bucket, datas);
                _current_bucket = (_current_bucket + 1) % _bucket_num;
                if (TxOpStatus_Code_Ok != status.error_code()) {
                    assert(datas.empty());
                    continue;
                } else {
                    assert(!datas.empty());

                    brpc::Controller cntl;
                    azino::storage::BatchStoreRequest req;
                    azino::storage::BatchStoreResponse resp;

                    for (auto &kv: datas) {
                        assert(!kv.tvs.empty());
                        for (auto &tv: kv.tvs) {
                            azino::storage::StoreData *d = req.add_datas();
                            d->set_ts(tv.first);
                            d->set_key(kv.key);
                            //req take over the "value *" and will free the memory later
                            d->set_allocated_value(tv.second);
                        }
                    }

                    _stub->BatchStore(&cntl, &req, &resp, NULL);

                    if (cntl.Failed()) {
                        LOG(WARNING) << "Controller failed error code: " << cntl.ErrorCode() << " error text: " << cntl.ErrorText();
                    } else if (resp.status().error_code() != storage::StorageStatus_Code_Ok) {
                        LOG(ERROR)
                        << "Fail to batch store mvcc data, error code: " << resp.status().error_code()
                        << " error msg: " << resp.status().error_message();
                    } else {
                        _txindex->ClearPersisted(current_bucket, datas);
                        // log will be printed by _txindex
                    }
                    return;
                }
            }

        }

        //Need to hold _mutex before call this.
        bool is_stopped(){
            return _bid != bthread_self();
        }

        static void *execute(void *args) {
            auto p = reinterpret_cast<Persistor *>(args);
            while (true) {
                bthread_usleep(FLAGS_persist_period * 1000);
                std::lock_guard<bthread::Mutex> lck(p->_mutex);// hold the _mutex when persist data.
                if (p->is_stopped()) {
                    break;
                }
                p->persist();
            }
            return nullptr;
        }


        std::unique_ptr<storage::StorageService_Stub> _stub;
        brpc::Channel _channel;
        TxIndex *_txindex;
        uint32_t _bucket_num;
        uint32_t _current_bucket;
        bthread::Mutex _mutex;
        bthread_t _bid;
    };


}}
#endif //AZINO_TXINDEX_INCLUDE_PERSISTOR_H
