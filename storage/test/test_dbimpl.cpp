#include <gtest/gtest.h>
#include <leveldb/db.h>
#include <string>

#include "storage.h"
#include "utils.h"

class DBImplTest : public testing::Test {
public:
    azino::storage::Storage* storage;
protected:
    void SetUp() {
        storage = azino::storage::Storage::DefaultStorage();
        storage->Open("TestDB");
    }
    void TearDown() {
        delete storage;
        leveldb::Options opt;
        leveldb::DestroyDB("TestDB", opt);
    }
};

TEST_F(DBImplTest, crud) {
    ASSERT_TRUE(storage->Put("hello", "world").error_code() == azino::storage::StorageStatus_Code_Ok);
    std::string s;
    ASSERT_EQ(azino::storage::StorageStatus_Code_Ok, storage->Get("hello", s).error_code());
    ASSERT_EQ("world", s);
    ASSERT_EQ(azino::storage::StorageStatus_Code_Ok, storage->Delete("hello").error_code());
    ASSERT_EQ(azino::storage::StorageStatus_Code_NotFound, storage->Get("hello", s).error_code());
    ASSERT_EQ(azino::storage::StorageStatus_Code_Ok, storage->Put("de", "ll").error_code());
    delete storage;
    storage = azino::storage::Storage::DefaultStorage();
    storage->Open("TestDB");
    ASSERT_EQ(azino::storage::StorageStatus_Code_Ok, storage->Get("de", s).error_code());
    ASSERT_EQ("ll", s);
}

TEST_F(DBImplTest, mvcc) {
    std::string seeked_key,seeked_value;
    uint64_t seeked_ts;
    ASSERT_TRUE(storage->Put("MVCDKEYmvccffffffffffffffff0", "world1").error_code() == azino::storage::StorageStatus_Code_Ok);
    ASSERT_TRUE(storage->MVCCGet("mvcc",0,seeked_value,seeked_ts).error_code()==azino::storage::StorageStatus_Code_NotFound);
    ASSERT_TRUE(storage->MVCCPut("mvcc",5,"123").error_code()==azino::storage::StorageStatus_Code_Ok);
    ASSERT_TRUE(storage->MVCCGet("mvcc",0,seeked_value,seeked_ts).error_code()==azino::storage::StorageStatus_Code_NotFound);
    ASSERT_TRUE(storage->MVCCGet("mv",10,seeked_value,seeked_ts).error_code()==azino::storage::StorageStatus_Code_NotFound);
    ASSERT_TRUE(storage->MVCCGet("mvcd",10,seeked_value,seeked_ts).error_code()==azino::storage::StorageStatus_Code_NotFound);
    ASSERT_TRUE(storage->MVCCGet("mvca",10,seeked_value,seeked_ts).error_code()==azino::storage::StorageStatus_Code_NotFound);
    ASSERT_TRUE(storage->MVCCGet("mvcc",10,seeked_value,seeked_ts).error_code()==azino::storage::StorageStatus_Code_Ok);
    ASSERT_EQ(seeked_value,"123");
    ASSERT_EQ(seeked_ts,5);
    ASSERT_TRUE(storage->MVCCPut("mvcc",15,"1234").error_code()==azino::storage::StorageStatus_Code_Ok);
    ASSERT_TRUE(storage->MVCCDelete("mvcc",20).error_code()==azino::storage::StorageStatus_Code_Ok);
    ASSERT_TRUE(storage->MVCCGet("mvcc",25,seeked_value,seeked_ts).error_code()==azino::storage::StorageStatus_Code_NotFound);
    ASSERT_TRUE(storage->MVCCGet("mvcc",18,seeked_value,seeked_ts).error_code()==azino::storage::StorageStatus_Code_Ok);
    ASSERT_EQ(seeked_value,"1234");
    ASSERT_EQ(seeked_ts,15);

    ASSERT_TRUE(storage->Seek("seek",seeked_key,seeked_value).error_code()==azino::storage::StorageStatus_Code_NotFound);
    ASSERT_TRUE(storage->Put("seek1", "world1").error_code() == azino::storage::StorageStatus_Code_Ok);
    ASSERT_TRUE(storage->Seek("seek",seeked_key,seeked_value).error_code()==azino::storage::StorageStatus_Code_Ok);
    ASSERT_EQ(seeked_key,"seek1");
    ASSERT_EQ(seeked_value,"world1");
    ASSERT_TRUE(storage->Put("seek", "world").error_code() == azino::storage::StorageStatus_Code_Ok);
    ASSERT_TRUE(storage->Seek("seek",seeked_key,seeked_value).error_code()==azino::storage::StorageStatus_Code_Ok);
    ASSERT_EQ(seeked_key,"seek");
    ASSERT_EQ(seeked_value,"world");

    ASSERT_EQ(azino::storage::StorageStatus_Code_Ok, storage->Delete("seek").error_code());
    ASSERT_EQ(azino::storage::StorageStatus_Code_Ok, storage->Delete("seek1").error_code());
    ASSERT_TRUE(storage->Seek("seek",seeked_key,seeked_value).error_code()==azino::storage::StorageStatus_Code_NotFound);
}
