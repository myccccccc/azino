#include "persistor.h"

#include <gflags/gflags.h>

DEFINE_int32(persist_period_ms, 1000, "persist period time");

namespace azino {
namespace txindex {

Persistor::Persistor(TxIndex *index, const std::string &storage_addr)
    : _txindex(index) {
    fn = Persistor::execute;
    brpc::ChannelOptions option;
    if (_channel.Init(storage_addr.c_str(), &option) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
    }
    _stub.reset(new storage::StorageService_Stub(&_channel));
}

void *Persistor::execute(void *args) {
    auto p = reinterpret_cast<Persistor *>(args);
    while (true) {
        bthread_usleep(FLAGS_persist_period_ms * 1000);
        {
            std::lock_guard<bthread::Mutex> lck(p->_mutex);
            if (p->_stopped) {
                break;
            }
        }
        p->persist();
    }
    return nullptr;
}

void Persistor::persist() {
    std::vector<DataToPersist> datas;
    auto status = _txindex->GetPersisting(datas);
    if (TxOpStatus_Code_Ok != status.error_code()) {
        assert(datas.empty());
    } else {
        assert(!datas.empty());
        brpc::Controller cntl;
        azino::storage::BatchStoreRequest req;
        azino::storage::BatchStoreResponse resp;

        for (auto &kv : datas) {
            assert(!kv.t2vs.empty());
            for (auto &tv : kv.t2vs) {
                azino::storage::StoreData *d = req.add_datas();
                d->set_key(kv.key);
                d->set_ts(tv.first.commit_ts());
                // req take over the "value *" and will free the memory later
                d->set_allocated_value(new Value(*tv.second));
            }
        }

        _stub->BatchStore(&cntl, &req, &resp, NULL);

        if (cntl.Failed()) {
            LOG(WARNING) << "Controller failed error code: " << cntl.ErrorCode()
                         << " error text: " << cntl.ErrorText();
        } else if (resp.status().error_code() !=
                   storage::StorageStatus_Code_Ok) {
            LOG(ERROR) << "Fail to batch store mvcc data, error code: "
                       << resp.status().error_code()
                       << " error msg: " << resp.status().error_message();
        } else {
            _txindex->ClearPersisted(datas);
            // log will be printed by _txindex
        }
        return;
    }
}
}  // namespace txindex
}  // namespace azino
