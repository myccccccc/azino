#ifndef AZINO_STORAGE_INCLUDE_UTILS_H
#define AZINO_STORAGE_INCLUDE_UTILS_H

#include "azino/kv.h"

namespace azino {
    namespace storage {

        class InternalKey {
        public:
            InternalKey(const std::string &user_key, TimeStamp ts, bool is_delete)
                    : _user_key(user_key), _ts(ts), _is_delete(is_delete), _valid(true) {}

            InternalKey(const std::string &internal_key)
                    : _user_key(), _ts(), _is_delete(), _valid() { decode(internal_key); } // decode here
            std::string Encode() const;

            bool IsDelete() const;

            std::string UserKey() const;

            TimeStamp TS() const;

            bool Valid() const;

        private:
            void decode(const std::string &internal_key);

            constexpr static const char *format_prefix = "MVCCKEY_%s";
            constexpr static const char *format_suffix = "%016lx_%c";
            constexpr static const char *format = "MVCCKEY_%s_%016lx_%c";
            static const int format_common_suffix_length = 19; // the common suffix's length, include timestamp and delete tag
            std::string _user_key;
            TimeStamp _ts;
            bool _is_delete;
            bool _valid; // set to false if decode error
        };



    } // namespace storage
} // namespace azino

#endif // AZINO_STORAGE_INCLUDE_UTILS_H
