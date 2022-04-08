#ifndef AZINO_STORAGE_INCLUDE_UTILS_H
#define AZINO_STORAGE_INCLUDE_UTILS_H

#include "storage.h"
#include "azino/kv.h"

namespace azino {
namespace storage {

    class InternalKey {
    public:
        InternalKey(const std::string &user_key, TimeStamp ts, bool is_delete)
            : _user_key(user_key), _ts(ts), _is_delete(is_delete), _valid(true) {}
        InternalKey(const std::string &internal_key)
            : _user_key(), _ts(), _is_delete(), _valid() {} // decode here
        std::string Encode() const;
        bool IsDelete() const;
        std::string UserKey() const;
        TimeStamp TS() const;
        bool Valid() const;
    private:
        const std::string _user_key;
        const TimeStamp _ts;
        const bool _is_delete;
        const bool _valid; // set to false if decode error
    };

    class MvccUtils{
    private:
        constexpr static const char *format = "MVCCKEY_%s_%016lx_%c";
        static const int format_common_suffix_length = 18; // the common suffix's length, include timestamp and delete tag
        static const int key_len = strlen(format) + format_common_suffix_length;
    public:
        enum KeyState {
            Mismatch,
            Deleted,
            OK
        };

        static KeyState GetKeyState(const std::string &internal_key, const std::string &found_key) {
            if(internal_key.length() != found_key.length()
                    || found_key.compare(0, found_key.length()-format_common_suffix_length,
                        internal_key, 0, internal_key.length()-format_common_suffix_length)) {
                return Mismatch;
            } else {
                if (found_key[found_key.size()-1]=='1') return Deleted;
                else return OK;
            }
        }

        static std::string Convert(const std::string &key, TimeStamp ts, bool is_delete) {
            char *buffer = new char[key.length() + key_len];
            sprintf(buffer, format, key.data(), ~ts,
                    is_delete ? '1' : '0'); // need to inverse timestamp because leveldb's seek find the bigger one
            auto ans = std::string(buffer);
            delete []buffer;
            return ans;
        }
    };
} // namespace storage
} // namespace azino

#endif // AZINO_STORAGE_INCLUDE_UTILS_H
