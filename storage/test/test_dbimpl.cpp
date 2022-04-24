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
    azino::TimeStamp seeked_ts;
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


TEST_F(DBImplTest, mvccbatch) {

    std::vector<azino::storage::Storage::Data> datas;

    auto *v0 = new azino::Value(), *v1 = new azino::Value(), *v2 = new azino::Value();
    v0->set_content("123");
    v0->set_is_delete(false);
    v1->set_content("234");
    v1->set_is_delete(false);
    v2->set_is_delete(true);
    std::vector<std::tuple<std::string, azino::TimeStamp, azino::Value *>> kvs{
            {"233", 5,  v0},
            {"233", 10, v1},
            {"233", 15, v2}
    };
    for (auto &tp: kvs) {
        datas.push_back(
                {std::get<0>(tp), std::get<2>(tp)->content(), std::get<1>(tp), std::get<2>(tp)->is_delete()});
    }

    ASSERT_EQ(storage->BatchStore(datas).error_code(), azino::storage::StorageStatus_Code_Ok);
    std::string seeked_value;
    azino::TimeStamp ts;
    ASSERT_EQ(storage->MVCCGet("233", 0, seeked_value, ts).error_code(), azino::storage::StorageStatus_Code_NotFound);

    ASSERT_EQ(storage->MVCCGet("233", 6, seeked_value, ts).error_code(), azino::storage::StorageStatus_Code_Ok);
    ASSERT_EQ(ts, 5);
    ASSERT_EQ(seeked_value, "123");

    ASSERT_EQ(storage->MVCCGet("234", 11, seeked_value, ts).error_code(), azino::storage::StorageStatus_Code_NotFound);
    ASSERT_EQ(storage->MVCCGet("233", 11, seeked_value, ts).error_code(), azino::storage::StorageStatus_Code_Ok);
    ASSERT_EQ(ts, 10);
    ASSERT_EQ(seeked_value, "234");

    ASSERT_EQ(storage->MVCCGet("233", 16, seeked_value, ts).error_code(), azino::storage::StorageStatus_Code_NotFound);
}
