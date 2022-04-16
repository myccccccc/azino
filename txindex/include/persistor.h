//
// Created by os on 4/15/22.
//
#include <butil/macros.h>
#include "memory"
#include "index.h"

#ifndef AZINO_TXINDEX_INCLUDE_PERSISTOR_H
#define AZINO_TXINDEX_INCLUDE_PERSISTOR_H
namespace azino {
    namespace txindex {
        class Persistor {
        public:
            Persistor(const uint32_t bucket_num,std::string storage_addr) = delete;


            DISALLOW_COPY_AND_ASSIGN(Persistor);

            //Start a new thread and monitor the data. Persist data periodically.
            void Start(){

            }

            //Stop monitor thread. Will be called when destruct.
            void Stop() {

            }

        private:
            bool is_running = false;
        };

        struct DataToPersist {
            UserKey key;
            TimeStamp maxTs;
            std::vector<std::pair<TimeStamp, Value *>> tvs;//Values are copied from origin and stored in heap
        };
    }
}
#endif //AZINO_TXINDEX_INCLUDE_PERSISTOR_H
