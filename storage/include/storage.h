#ifndef AZINO_STORAGE_INCLUDE_STORAGE_H
#define AZINO_STORAGE_INCLUDE_STORAGE_H

#include <string>
#include <sstream>
#include <butil/macros.h>

#include "azino/kv.h"
#include "service/storage/storage.pb.h"
#include "utils.h"

namespace azino {
namespace storage {

    class Storage {
    public:
        // return the default Storage impl
        static Storage* DefaultStorage();

        Storage() = default;
        DISALLOW_COPY_AND_ASSIGN(Storage);
        virtual ~Storage() = default;

        virtual StorageStatus Open(const std::string& name) = 0;

        // Set the database entry for "key" to "value".  Returns OK on success,
        // and a non-OK status on error.
        virtual StorageStatus Put(const std::string& key,
                           const std::string& value) = 0;

        // Remove the database entry (if any) for "key".  Returns OK on
        // success, and a non-OK status on error.  It is not an error if "key"
        // did not exist in the database.
        virtual StorageStatus Delete(const std::string& key) = 0;

        // If the database contains an entry for "key" store the
        // corresponding value in *value and return OK.
        //
        // If there is no entry for "key" leave *value unchanged and return
        // a status for which Status::IsNotFound() returns true.
        //
        // May return some other Status on an error.
        virtual StorageStatus Get(const std::string& key,
                           std::string& value) = 0;

        // If the database contains keys whose bitwise value are equal or bigger than "key", store the
        // biggest key and value in *found_key and *value and return OK.
        //
        // If there is no entry for "key" leave *found_key and *value unchanged and return
        // a status for which Status::IsNotFound() returns true.
        //
        // May return some other Status on an error.
        virtual StorageStatus Seek(const std::string &key,std::string &found_key,std::string &value) = 0;

        struct Data {
            const std::string key;
            const std::string value;
            const TimeStamp ts;
            const bool is_delete;
        };
        // Add a series of database entries for "key" to "value" with timestamp "ts" and tag "is_delete".  Returns OK on success,
        // and a non-OK status on error.
        virtual StorageStatus BatchStore(const std::vector<Data> &datas) = 0;
        // Add a database entry for "key" to "value" with timestamp "ts".  Returns OK on success,
        // and a non-OK status on error.

        virtual StorageStatus MVCCPut(const std::string &key, TimeStamp ts, const std::string& value) {
            auto internal_key = InternalKey(key, ts, false);
            StorageStatus ss = Put(internal_key.Encode(), value);
            return ss;
        }
        // Add a database entry (if any) for "key" with timestamp "ts" to indicate the value is deleted.  Returns OK on
        // success, and a non-OK status on error.  It is not an error if "key"
        // did not exist in the database.
        virtual StorageStatus MVCCDelete(const std::string &key, TimeStamp ts) {
            auto internal_key = InternalKey(key, ts, true);
            StorageStatus ss = Put(internal_key.Encode(), "");
            return ss;
        }
        // If the database contains an entry for "key" and has a smaller timestamp, store the
        // corresponding value in value and return OK.
        //
        // If there is no entry for "key" or the key is marked deleted leave value unchanged and return
        // a status for which Status::IsNotFound() returns true.
        //
        // May return some other Status on an error.
        virtual StorageStatus MVCCGet(const std::string& key, TimeStamp ts, std::string &value, TimeStamp &seeked_ts) {
            auto internal_key = InternalKey(key, ts, false);
            std::string found_key, found_value;
            StorageStatus ss = Seek(internal_key.Encode(), found_key, found_value);
            std::stringstream strs;
            if (ss.error_code() != StorageStatus::Ok) {
                return ss;
            } else {
                auto found_internal_key = InternalKey(found_key);
                bool isValid = found_internal_key.Valid();
                bool isMatch = found_internal_key.UserKey() == key;
                bool isDeleted = found_internal_key.IsDelete();
                if (!isValid) {
                    ss.set_error_code(StorageStatus_Code_Corruption);
                    return ss;
                } else if (!isMatch) {
                    ss.set_error_code(StorageStatus_Code_NotFound);
                    strs << " Fail to find mvcc key: " << key
                         << " read ts: " << ts
                         << " found key: " << found_internal_key.UserKey()
                         << " found ts: " << seeked_ts
                         << " found value: " << found_value;
                    ss.set_error_message(strs.str());
                    return ss;
                } else if (isDeleted) {
                    ss.set_error_code(StorageStatus_Code_NotFound);
                    strs << " Fail to find mvcc key: " << key
                         << " read ts: " << ts
                         << " found ts: " << seeked_ts
                         << " who's value is deleted";
                    ss.set_error_message(strs.str());
                    return ss;
                } else {
                    value = found_value;
                    seeked_ts = found_internal_key.TS();
                    strs << " Success to find mvcc key: " << key
                         << " read ts: " << ts
                         << " found ts: " << seeked_ts
                         << " found value: " << found_value;
                    ss.set_error_message(strs.str());
                    return ss;
                }
            }
        }

    };

} // namespace storage
} // namespace azino

#endif // AZINO_STORAGE_INCLUDE_STORAGE_H
