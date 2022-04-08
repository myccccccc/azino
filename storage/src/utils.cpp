//
// Created by os on 4/8/22.
//
#include <cstring>
#include <memory>
#include "utils.h"

namespace azino {
    namespace storage {
        std::string InternalKey::Encode() const {
            static int key_len = strlen(format) + format_common_suffix_length;
            char *buffer = new char[_user_key.length() + key_len];
            sprintf(buffer, format, _user_key.data(), ~_ts,
                    _is_delete ? '1' : '0'); // need to inverse timestamp because leveldb's seek find the bigger one
            auto ans = std::string(buffer);
            delete[]buffer;
            return ans;
        }

        bool InternalKey::Valid() const {
            return _valid;
        }

        bool InternalKey::IsDelete() const {
            return _is_delete;
        }

        std::string InternalKey::UserKey() const {
            return _user_key;
        }

        TimeStamp InternalKey::TS() const {
            return _ts;
        }

        void InternalKey::decode(const std::string &internal_key) {
            if (internal_key.length() <= format_common_suffix_length) {
                _valid = false;
                return;
            }
            std::unique_ptr<char[]> buf_in(new char[internal_key.length() + 1]);
            std::unique_ptr<char[]> buf_out(new char[internal_key.length() + 1]);
            strcpy(buf_in.get(), internal_key.data());
            buf_in[internal_key.length() - format_common_suffix_length] = 0;
            buf_in[internal_key.length()] = 0;
            if (sscanf(buf_in.get(), format_prefix, buf_out.get()) != 1) {
                _valid = false;
                return;
            }
            char buf_is_deleted;
            TimeStamp buf_ts;
            if (sscanf(buf_in.get() + internal_key.length() - format_common_suffix_length + 1, format_suffix, &buf_ts,
                       &buf_is_deleted) != 2) {
                _valid = false;
                return;
            }
            _valid = true;
            _user_key = std::string(buf_out.get());
            _is_delete = buf_is_deleted != '0';
            _ts = ~buf_ts;
        }
    }
}