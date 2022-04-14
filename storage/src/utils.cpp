#include <cstring>
#include <cassert>
#include <memory>

#include "utils.h"

namespace azino {
namespace storage {
    std::string InternalKey::Encode() const {
        char *buf = new char[strlen(format_prefix) + _user_key.length() + ts_length + delete_tag_length + 1]();
        char *index = buf;

        strcpy(buf, format_prefix);
        index += strlen(format_prefix);

        unsigned n = sprintf(index, format_user_key, _user_key.data());
        index += n;
        assert(n == _user_key.length());

        n = sprintf(index, format_ts, ~_ts); // need to inverse timestamp because leveldb's seek find the bigger one
        index += n;
        assert(n == ts_length);

        n = sprintf(index, format_delete_tag, _is_delete ? '1' : '0');
        index += n;
        assert(n == delete_tag_length);

        auto ans = std::string(buf);
        delete []buf;
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
        if (internal_key.length() < strlen(format_prefix) + ts_length + delete_tag_length) {
            _valid = false;
            return;
        }
        if (internal_key.compare(0, strlen(format_prefix), format_prefix, strlen(format_prefix))) {
            _valid = false;
            return;
        }
        auto user_key_length = internal_key.length() - (strlen(format_prefix) + ts_length + delete_tag_length);
        TimeStamp buf_ts;
        char buf_delete_tag;
        std::unique_ptr<char []> buf_user_key( new char[user_key_length + 1]());

        auto index = internal_key.data();
        index += strlen(format_prefix);

        strncpy(buf_user_key.get(), index, user_key_length);
        index += user_key_length;

        if (1 != sscanf(index, format_ts, &buf_ts)) {
            _valid = false;
            return;
        }
        index += ts_length;

        if (1 != sscanf(index, format_delete_tag, &buf_delete_tag)) {
            _valid = false;
            return;
        }
        index += delete_tag_length;

        _valid = true;
        _user_key = std::string(buf_user_key.get());
        _is_delete = buf_delete_tag != '0';
        _ts = ~buf_ts;
    }
} // namespace storage
} // namespace azino
