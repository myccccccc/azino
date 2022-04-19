#include <gtest/gtest.h>
#include <butil/hash.h>
#include <bthread/bthread.h>
#include <brpc/channel.h>
#include "persistor.h"
#include "index.h"
#include "service/storage/storage.pb.h"

class TxIndexImplTest : public testing::Test {
public:
    TxIndexImplTest() {
        bthread_mutex_init(&_m, nullptr);
        bthread_cond_init(&_cv, nullptr);
    }

    azino::txindex::TxIndex* ti;
    azino::TxIdentifier t1;
    azino::TxIdentifier t2;
    std::string k1 = "key1";
    azino::Value v1;
    std::string k2 = "key2";
    azino::Value v2;

    void dummyCallback() {
        LOG(INFO) << "Calling dummy call back.";
        bthread_cond_signal(&_cv);
    }

    void waitDummyCallback() {
        bthread_mutex_lock(&_m);
        bthread_cond_wait(&_cv, &_m);
        _called = true;
        bthread_mutex_unlock(&_m);
    }

    bool Called() {
        return _called;
    }

    void UnCalled() {
        _called = false;
    }

protected:
    void SetUp() {
        UnCalled();
        FLAGS_enable_persistor = false;
        ti = azino::txindex::TxIndex::DefaultTxIndex();
        t1.set_start_ts(1);
        t2.set_start_ts(2);
        v1.set_content("tx1value");
        v2.set_content("tx2value");
    }
    void TearDown() {
        delete ti;
    }

private:
    bool _called;
    bthread_mutex_t _m;
    bthread_cond_t _cv;
};



TEST_F(TxIndexImplTest, dummy) {
    ASSERT_EQ(butil::Hash("abc"), 3535673738);
    ASSERT_EQ(butil::Hash("hello"), 2963130491);
    azino::TxIdentifier id;
    id.set_start_ts(123);
    id.set_commit_ts(456);
    LOG(INFO) << id.ShortDebugString();
}

TEST_F(TxIndexImplTest, write_intent_ok) {
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v1, t1).error_code());

    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k2, t1, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k2, v1, t1).error_code());
}

TEST_F(TxIndexImplTest, write_intent_conflicts) {
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_WriteConflicts, ti->WriteIntent(k1, v2, t2).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Clean(k1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v2, t2).error_code());

    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k2, t1, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_WriteConflicts, ti->WriteIntent(k2, v2, t2).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Clean(k2, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k2, v2, t2).error_code());
}

TEST_F(TxIndexImplTest, write_intent_too_late) {
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v2, t2).error_code());
    t2.set_commit_ts(4);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t2).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_WriteTooLate, ti->WriteIntent(k1, v1, t1).error_code());
}

TEST_F(TxIndexImplTest, write_lock_ok) {
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k1, t1, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k1, t1, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());

    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k2, v1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k2, t1, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
}

TEST_F(TxIndexImplTest, write_lock_block) {
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_WriteBlock, ti->WriteLock(k1, t2, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Clean(k1, t1).error_code());
    waitDummyCallback();
    ASSERT_TRUE(Called());
    UnCalled();

    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k2, t1, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_WriteBlock, ti->WriteLock(k2, t2, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Clean(k2, t1).error_code());
    waitDummyCallback();
    ASSERT_TRUE(Called());
}

TEST_F(TxIndexImplTest, write_lock_too_late) {
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v2, t2).error_code());
    t2.set_commit_ts(4);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t2).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_WriteTooLate, ti->WriteLock(k1, t1, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
}

TEST_F(TxIndexImplTest, clean_not_exist) {
    ASSERT_EQ(azino::TxOpStatus_Code_CleanNotExist, ti->Clean(k1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k1, t2, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_CleanNotExist, ti->Clean(k1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v2, t2).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_CleanNotExist, ti->Clean(k1, t1).error_code());
}

TEST_F(TxIndexImplTest, clean_ok) {
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k1, t1, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Clean(k1, t1).error_code());

    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k2, t1, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k2, v1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Clean(k2, t1).error_code());
}

TEST_F(TxIndexImplTest, commit_not_exist) {
    ASSERT_EQ(azino::TxOpStatus_Code_CommitNotExist, ti->Commit(k1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k1, t2, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_CommitNotExist, ti->Commit(k1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_CommitNotExist, ti->Commit(k1, t2).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v2, t2).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_CommitNotExist, ti->Commit(k1, t1).error_code());
}

TEST_F(TxIndexImplTest, commit_ok) {
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v1, t1).error_code());
    t1.set_commit_ts(3);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t1).error_code());

    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k2, t2, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k2, v2, t2).error_code());
    t2.set_commit_ts(4);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k2, t2).error_code());
}

TEST_F(TxIndexImplTest, read_ok) {
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v1, t1).error_code());
    t1.set_commit_ts(3);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t1).error_code());
    azino::Value read_value;
    azino::TxIdentifier read_tx;
    read_tx.set_start_ts(4);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Read(k1, read_value, read_tx, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(v1.content(), read_value.content());

    azino::TxIdentifier t3;
    t3.set_start_ts(4);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v2, t3).error_code());
    t3.set_commit_ts(5);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t3).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Read(k1, read_value, read_tx, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(v1.content(), read_value.content());
    read_tx.set_start_ts(5);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Read(k1, read_value, read_tx, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(v2.content(), read_value.content());
}

TEST_F(TxIndexImplTest, read_block) {
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v1, t1).error_code());
    t1.set_commit_ts(2);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t1).error_code());
    azino::TxIdentifier read_tx_3;
    read_tx_3.set_start_ts(3);
    azino::TxIdentifier read_tx_6;
    read_tx_6.set_start_ts(6);
    azino::TxIdentifier t3;
    t3.set_start_ts(4);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k1, t3, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    azino::Value read_value;
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Read(k1, read_value, read_tx_3, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(v1.content(), read_value.content());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Read(k1, read_value, read_tx_6, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(v1.content(), read_value.content());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v2, t3).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Read(k1, read_value, read_tx_3, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(v1.content(), read_value.content());
    ASSERT_EQ(azino::TxOpStatus_Code_ReadBlock, ti->Read(k1, read_value, read_tx_6, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    t3.set_commit_ts(5);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t3).error_code());
    waitDummyCallback();
    ASSERT_TRUE(Called());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Read(k1, read_value, read_tx_3, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(v1.content(), read_value.content());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Read(k1, read_value, read_tx_6, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(v2.content(), read_value.content());
}

TEST_F(TxIndexImplTest, read_not_exist) {
    azino::Value read_value;
    ASSERT_EQ(azino::TxOpStatus_Code_ReadNotExist, ti->Read(k1, read_value, t1, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k1, t1, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_ReadNotExist, ti->Read(k1, read_value, t1, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_ReadNotExist, ti->Read(k1, read_value, t1, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    t1.set_commit_ts(4);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_ReadNotExist, ti->Read(k1, read_value, t1, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    azino::TxIdentifier read_tx_3;
    read_tx_3.set_start_ts(3);
    ASSERT_EQ(azino::TxOpStatus_Code_ReadNotExist, ti->Read(k1, read_value, read_tx_3, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    azino::TxIdentifier read_tx_6;
    read_tx_6.set_start_ts(6);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Read(k1, read_value, read_tx_6, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(v1.content(), read_value.content());
}


TEST_F(TxIndexImplTest, persist) {
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v1, t1).error_code());
    t1.set_commit_ts(3);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t1).error_code());
    t2.set_start_ts(4);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteLock(k1, t2, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v2, t2).error_code());
    t2.set_commit_ts(5);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t2).error_code());
    azino::Value read_value;
    azino::TxIdentifier read_tx_3;
    read_tx_3.set_start_ts(3);
    azino::TxIdentifier read_tx_6;
    read_tx_6.set_start_ts(6);
    ASSERT_EQ(ti->Read(k1, read_value, read_tx_3, NULL).error_code(), azino::TxOpStatus_Code_Ok);
    ASSERT_EQ(v1.content(), read_value.content());
    ASSERT_EQ(ti->Read(k1, read_value, read_tx_6, NULL).error_code(), azino::TxOpStatus_Code_Ok);
    ASSERT_EQ(v2.content(), read_value.content());
    std::vector<azino::txindex::DataToPersist> datas;
    auto bucket_num = butil::Hash(k1) % FLAGS_latch_bucket_num;
    ASSERT_EQ(ti->GetPersisting(bucket_num, datas).error_code(), azino::TxOpStatus_Code_Ok);
    ASSERT_EQ(datas.size(), 1);
    ASSERT_EQ(datas[0].tvs.size(), 2);
    for (auto &x: datas) {
        for (auto &v: x.tvs) {
            delete v.second;
        }
    }
    ASSERT_EQ(ti->ClearPersisted(bucket_num, datas).error_code(), azino::TxOpStatus_Code_Ok);
    ASSERT_EQ(ti->ClearPersisted(bucket_num, datas).error_code(), azino::TxOpStatus_Code_ClearRepeat);
    datas.clear();
    ASSERT_EQ(ti->GetPersisting(bucket_num, datas).error_code(), azino::TxOpStatus_Code_NoneToPersist);
    ASSERT_EQ(datas.size(), 0);
    ASSERT_EQ(ti->Read(k1, read_value, read_tx_3, NULL).error_code(), azino::TxOpStatus_Code_ReadNotExist);
    ASSERT_EQ(ti->Read(k1, read_value, read_tx_6, NULL).error_code(), azino::TxOpStatus_Code_ReadNotExist);
}
