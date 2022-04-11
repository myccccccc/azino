#include <leveldb/db.h>
#include <memory>

#include "storage.h"
#include "utils.h"

namespace azino {
namespace storage {
namespace {

    StorageStatus LevelDBStatus(const leveldb::Status &lss);
    
    class LevelDBImpl : public Storage {
    public:
        LevelDBImpl() : _leveldbptr(nullptr) {}
        DISALLOW_COPY_AND_ASSIGN(LevelDBImpl);
        virtual ~LevelDBImpl() = default;

        virtual StorageStatus Open(const std::string& name) override {
            if (_leveldbptr != nullptr) {
                StorageStatus ss;
                ss.set_error_code(StorageStatus::InvalidArgument);
                ss.set_error_message("Already opened an leveldb");
                return ss;
            }
            leveldb::DB* leveldbptr;
            leveldb::Options opt;
            opt.create_if_missing = true;
            leveldb::Status leveldbstatus;
            leveldbstatus =  leveldb::DB::Open(opt, name, &leveldbptr);
            if (leveldbstatus.ok()) {
                _leveldbptr.reset(leveldbptr);
            }
            return LevelDBStatus(leveldbstatus);
        }

        // Set the database entry for "key" to "value".  Returns OK on success,
        // and a non-OK status on error.
        virtual StorageStatus Put(const std::string& key, const std::string& value) override {
            if (_leveldbptr == nullptr) {
                StorageStatus ss;
                ss.set_error_code(StorageStatus::InvalidArgument);
                ss.set_error_message("Already opened an leveldb");
                return ss;
            }
            leveldb::WriteOptions opts;
            opts.sync = true;
            leveldb::Status leveldbstatus;
            leveldbstatus = _leveldbptr->Put(opts, key, value);
            return LevelDBStatus(leveldbstatus);
        }

        // Remove the database entry (if any) for "key".  Returns OK on
        // success, and a non-OK status on error.  It is not an error if "key"
        // did not exist in the database.
        virtual StorageStatus Delete(const std::string& key) override {
            if (_leveldbptr == nullptr) {
                StorageStatus ss;
                ss.set_error_code(StorageStatus::InvalidArgument);
                ss.set_error_message("Already opened an leveldb");
                return ss;
            }
            leveldb::WriteOptions opts;
            opts.sync = true;
            leveldb::Status leveldbstatus;
            leveldbstatus = _leveldbptr->Delete(opts, key);
            return LevelDBStatus(leveldbstatus);
        }

        // If the database contains an entry for "key" store the
        // corresponding value in *value and return OK.
        //
        // If there is no entry for "key" leave *value unchanged and return
        // a status for which Status::IsNotFound() returns true.
        //
        // May return some other Status on an error.
        virtual StorageStatus Get(const std::string& key, std::string& value) override {
            if (_leveldbptr == nullptr) {
                StorageStatus ss;
                ss.set_error_code(StorageStatus::InvalidArgument);
                ss.set_error_message("Already opened an leveldb");
                return ss;
            }
            leveldb::ReadOptions opt;
            opt.verify_checksums = true;
            leveldb::Status leveldbstatus;
            leveldbstatus = _leveldbptr->Get(opt, key, &value);
            return LevelDBStatus(leveldbstatus);
        }


        virtual StorageStatus Seek(const std::string &key, std::string &found_key, std::string &value) override {
            if (_leveldbptr == nullptr) {
                StorageStatus ss;
                ss.set_error_code(StorageStatus::InvalidArgument);
                ss.set_error_message("Already opened an leveldb");
                return ss;
            }
            leveldb::ReadOptions opt;
            opt.verify_checksums = true;
            std::unique_ptr<leveldb::Iterator> iter(_leveldbptr->NewIterator(opt));
            iter->Seek(key);
            if (iter->Valid()) {
                found_key = iter->key().ToString();
                value = iter->value().ToString();
                return LevelDBStatus(iter->status());
            } else {
                if (iter->status().ok()) {
                    return LevelDBStatus(leveldb::Status::NotFound("Iter status: " + iter->status().ToString()));
                } else {
                    return LevelDBStatus(iter->status());
                }
            }

        }

   private:
        std::unique_ptr<leveldb::DB> _leveldbptr;
    };

    StorageStatus LevelDBStatus(const leveldb::Status &lss) {
        StorageStatus ss;
        if (lss.ok()) {
            ss.set_error_code(StorageStatus::Ok);
        } else if (lss.IsCorruption()) {
            ss.set_error_code(StorageStatus::Corruption);
            ss.set_error_message(lss.ToString());
        } else if (lss.IsIOError()) {
            ss.set_error_code(StorageStatus::IOError);
            ss.set_error_message(lss.ToString());
        } else if (lss.IsInvalidArgument()) {
            ss.set_error_code(StorageStatus::InvalidArgument);
            ss.set_error_message(lss.ToString());
        } else if (lss.IsNotFound()) {
            ss.set_error_code(StorageStatus::NotFound);
            ss.set_error_message(lss.ToString());
        } else {
            ss.set_error_code(StorageStatus::NotSupported);
            ss.set_error_message(lss.ToString());
        }
        return ss;
    }
} // namespace

    Storage *Storage::DefaultStorage() {
        return new LevelDBImpl();
    }
} // namespace storage
} // namespace azino
