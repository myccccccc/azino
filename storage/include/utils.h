//
// Created by os on 4/6/22.
//

#ifndef AZINO_STORAGE_INCLUDE_UTILS_H
#define AZINO_STORAGE_INCLUDE_UTILS_H
#include "storage.h"
namespace azino {
    namespace storage {
        class MvccUtils{
        private:
            constexpr static char *format="MVCCKEY_%s_%016lx_%c";
            constexpr static int format_common_suffix_length = 18;//the common suffix's length, include timestamp and delete tag
        public:
            enum KeyState{
                Mismatch,
                Deleted,
                OK
            };

            //return the state of found_key, if state is ok, store found_key's timestamp in ts
            static KeyState Decode(const std::string &origin_key,const int internal_key_length,const std::string &found_key,uint64_t &ts){
                if(internal_key_length!=found_key.length()){
                    return Mismatch;
                }

                std::unique_ptr<char[]> buffer( new char[internal_key_length+1]);
                uint64_t buf_ts;
                uint64_t buf_is_deleted;
                if(3!=sscanf(found_key.data(),format,buffer.get(),&buf_ts,&buf_is_deleted)){
                    return Mismatch;
                }
                if(strcmp(buffer.get(),origin_key.data()) != 0){
                    return Mismatch;
                }
                if(buf_is_deleted!='0'){
                    return Deleted;
                }
                ts = ~buf_ts;
                return OK;
            }
            static std::string Encode(const std::string &key, uint64_t ts, bool is_deleted) {

                static int key_len = strlen(format) + format_common_suffix_length;

                char *buffer = new char[key.length() + key_len];
                sprintf(buffer, format, key.data(), ~ts,
                        is_deleted ? '1' : '0');//need to inverse timestamp because leveldb's seek find the bigger one
                auto ans = std::string(buffer);
                delete[]buffer;
                return ans;
            }



        };
    }
}
#endif //AZINO_STORAGE_INCLUDE_UTILS_H
