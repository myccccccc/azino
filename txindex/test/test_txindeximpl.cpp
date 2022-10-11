#include <brpc/channel.h>
#include <bthread/bthread.h>
#include <butil/hash.h>
#include <gtest/gtest.h>

#include "index.h"
#include "persistor.h"
#include "reporter.h"
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
        bthread_mutex_lock(&_m);
        bthread_cond_signal(&_cv);
        _called = true;
        bthread_mutex_unlock(&_m);
    }

    void waitDummyCallback() {
        bthread_mutex_lock(&_m);
        while (!_called) {
            bthread_cond_wait(&_cv, &_m);
        }
        bthread_mutex_unlock(&_m);
    }

    bool Called() { return _called; }

    void UnCalled() { _called = false; }

   protected:
    void SetUp() {
        UnCalled();
        FLAGS_latch_bucket_num = 1;
        ti = azino::txindex::TxIndex::DefaultTxIndex(
            "127.0.0.1:1080");  //  A dummy address
        t1.set_start_ts(1);
        t2.set_start_ts(2);
        v1.set_content("tx1value");
        v2.set_content("tx2value");
    }
    void TearDown() { delete ti; }

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
    std::vector<azino::txindex::Dep> deps;
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k1, v1, t1, deps).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k1, v1, t1, deps).error_code());

    ASSERT_EQ(
        azino::TxOpStatus_Code_Ok,
        ti->WriteLock(k2, t1, std::bind(&TxIndexImplTest::dummyCallback, this),
                      deps)
            .error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k2, v1, t1, deps).error_code());
}

TEST_F(TxIndexImplTest, write_intent_conflicts) {
    std::vector<azino::txindex::Dep> deps;
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k1, v1, t1, deps).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_WriteConflicts,
              ti->WriteIntent(k1, v2, t2, deps).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Clean(k1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k1, v2, t2, deps).error_code());

    ASSERT_EQ(
        azino::TxOpStatus_Code_Ok,
        ti->WriteLock(k2, t1, std::bind(&TxIndexImplTest::dummyCallback, this),
                      deps)
            .error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_WriteConflicts,
              ti->WriteIntent(k2, v2, t2, deps).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Clean(k2, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k2, v2, t2, deps).error_code());
}

TEST_F(TxIndexImplTest, write_intent_too_late) {
    std::vector<azino::txindex::Dep> deps;
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k1, v2, t2, deps).error_code());
    t2.set_commit_ts(4);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t2).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_WriteTooLate,
              ti->WriteIntent(k1, v1, t1, deps).error_code());
}

TEST_F(TxIndexImplTest, write_lock_ok) {
    std::vector<azino::txindex::Dep> deps;
    ASSERT_EQ(
        azino::TxOpStatus_Code_Ok,
        ti->WriteLock(k1, t1, std::bind(&TxIndexImplTest::dummyCallback, this),
                      deps)
            .error_code());
    ASSERT_EQ(
        azino::TxOpStatus_Code_Ok,
        ti->WriteLock(k1, t1, std::bind(&TxIndexImplTest::dummyCallback, this),
                      deps)
            .error_code());

    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k2, v1, t1, deps).error_code());
    ASSERT_EQ(
        azino::TxOpStatus_Code_Ok,
        ti->WriteLock(k2, t1, std::bind(&TxIndexImplTest::dummyCallback, this),
                      deps)
            .error_code());
}

TEST_F(TxIndexImplTest, write_lock_block) {
    std::vector<azino::txindex::Dep> deps;
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k1, v1, t1, deps).error_code());
    ASSERT_EQ(
        azino::TxOpStatus_Code_WriteBlock,
        ti->WriteLock(k1, t2, std::bind(&TxIndexImplTest::dummyCallback, this),
                      deps)
            .error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Clean(k1, t1).error_code());
    waitDummyCallback();
    ASSERT_TRUE(Called());
    UnCalled();

    ASSERT_EQ(
        azino::TxOpStatus_Code_Ok,
        ti->WriteLock(k2, t1, std::bind(&TxIndexImplTest::dummyCallback, this),
                      deps)
            .error_code());
    ASSERT_EQ(
        azino::TxOpStatus_Code_WriteBlock,
        ti->WriteLock(k2, t2, std::bind(&TxIndexImplTest::dummyCallback, this),
                      deps)
            .error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Clean(k2, t1).error_code());
    waitDummyCallback();
    ASSERT_TRUE(Called());
}

TEST_F(TxIndexImplTest, write_lock_too_late) {
    std::vector<azino::txindex::Dep> deps;
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k1, v2, t2, deps).error_code());
    t2.set_commit_ts(4);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t2).error_code());
    ASSERT_EQ(
        azino::TxOpStatus_Code_WriteTooLate,
        ti->WriteLock(k1, t1, std::bind(&TxIndexImplTest::dummyCallback, this),
                      deps)
            .error_code());
}

TEST_F(TxIndexImplTest, clean_not_exist) {
    std::vector<azino::txindex::Dep> deps;
    ASSERT_EQ(azino::TxOpStatus_Code_CleanNotExist,
              ti->Clean(k1, t1).error_code());
    ASSERT_EQ(
        azino::TxOpStatus_Code_Ok,
        ti->WriteLock(k1, t2, std::bind(&TxIndexImplTest::dummyCallback, this),
                      deps)
            .error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_CleanNotExist,
              ti->Clean(k1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k1, v2, t2, deps).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_CleanNotExist,
              ti->Clean(k1, t1).error_code());
}

TEST_F(TxIndexImplTest, clean_ok) {
    std::vector<azino::txindex::Dep> deps;
    ASSERT_EQ(
        azino::TxOpStatus_Code_Ok,
        ti->WriteLock(k1, t1, std::bind(&TxIndexImplTest::dummyCallback, this),
                      deps)
            .error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Clean(k1, t1).error_code());

    ASSERT_EQ(
        azino::TxOpStatus_Code_Ok,
        ti->WriteLock(k2, t1, std::bind(&TxIndexImplTest::dummyCallback, this),
                      deps)
            .error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k2, v1, t1, deps).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Clean(k2, t1).error_code());
}

TEST_F(TxIndexImplTest, commit_not_exist) {
    std::vector<azino::txindex::Dep> deps;
    ASSERT_EQ(azino::TxOpStatus_Code_CommitNotExist,
              ti->Commit(k1, t1).error_code());
    ASSERT_EQ(
        azino::TxOpStatus_Code_Ok,
        ti->WriteLock(k1, t2, std::bind(&TxIndexImplTest::dummyCallback, this),
                      deps)
            .error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_CommitNotExist,
              ti->Commit(k1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_CommitNotExist,
              ti->Commit(k1, t2).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k1, v2, t2, deps).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_CommitNotExist,
              ti->Commit(k1, t1).error_code());
}

TEST_F(TxIndexImplTest, commit_ok) {
    std::vector<azino::txindex::Dep> deps;
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k1, v1, t1, deps).error_code());
    t1.set_commit_ts(3);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t1).error_code());

    ASSERT_EQ(
        azino::TxOpStatus_Code_Ok,
        ti->WriteLock(k2, t2, std::bind(&TxIndexImplTest::dummyCallback, this),
                      deps)
            .error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k2, v2, t2, deps).error_code());
    t2.set_commit_ts(4);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k2, t2).error_code());
}

TEST_F(TxIndexImplTest, read_ok) {
    std::vector<azino::txindex::Dep> deps;
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k1, v1, t1, deps).error_code());
    t1.set_commit_ts(3);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t1).error_code());
    azino::Value read_value;
    azino::TxIdentifier read_tx;
    read_tx.set_start_ts(4);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->Read(k1, read_value, read_tx,
                       std::bind(&TxIndexImplTest::dummyCallback, this), deps)
                  .error_code());
    ASSERT_EQ(v1.content(), read_value.content());

    azino::TxIdentifier t3;
    t3.set_start_ts(4);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k1, v2, t3, deps).error_code());
    t3.set_commit_ts(5);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t3).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->Read(k1, read_value, read_tx,
                       std::bind(&TxIndexImplTest::dummyCallback, this), deps)
                  .error_code());
    ASSERT_EQ(v1.content(), read_value.content());
    read_tx.set_start_ts(5);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->Read(k1, read_value, read_tx,
                       std::bind(&TxIndexImplTest::dummyCallback, this), deps)
                  .error_code());
    ASSERT_EQ(v2.content(), read_value.content());
}

TEST_F(TxIndexImplTest, read_block) {
    std::vector<azino::txindex::Dep> deps;
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k1, v1, t1, deps).error_code());
    t1.set_commit_ts(2);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t1).error_code());
    azino::TxIdentifier read_tx_3;
    read_tx_3.set_start_ts(3);
    azino::TxIdentifier read_tx_6;
    read_tx_6.set_start_ts(6);
    azino::TxIdentifier t3;
    t3.set_start_ts(4);
    ASSERT_EQ(
        azino::TxOpStatus_Code_Ok,
        ti->WriteLock(k1, t3, std::bind(&TxIndexImplTest::dummyCallback, this),
                      deps)
            .error_code());
    azino::Value read_value;
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->Read(k1, read_value, read_tx_3,
                       std::bind(&TxIndexImplTest::dummyCallback, this), deps)
                  .error_code());
    ASSERT_EQ(v1.content(), read_value.content());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->Read(k1, read_value, read_tx_6,
                       std::bind(&TxIndexImplTest::dummyCallback, this), deps)
                  .error_code());
    ASSERT_EQ(v1.content(), read_value.content());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k1, v2, t3, deps).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->Read(k1, read_value, read_tx_3,
                       std::bind(&TxIndexImplTest::dummyCallback, this), deps)
                  .error_code());
    ASSERT_EQ(v1.content(), read_value.content());
    ASSERT_EQ(azino::TxOpStatus_Code_ReadBlock,
              ti->Read(k1, read_value, read_tx_6,
                       std::bind(&TxIndexImplTest::dummyCallback, this), deps)
                  .error_code());
    t3.set_commit_ts(5);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t3).error_code());
    waitDummyCallback();
    ASSERT_TRUE(Called());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->Read(k1, read_value, read_tx_3,
                       std::bind(&TxIndexImplTest::dummyCallback, this), deps)
                  .error_code());
    ASSERT_EQ(v1.content(), read_value.content());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->Read(k1, read_value, read_tx_6,
                       std::bind(&TxIndexImplTest::dummyCallback, this), deps)
                  .error_code());
    ASSERT_EQ(v2.content(), read_value.content());
}

TEST_F(TxIndexImplTest, read_not_exist) {
    std::vector<azino::txindex::Dep> deps;
    azino::Value read_value;
    ASSERT_EQ(azino::TxOpStatus_Code_ReadNotExist,
              ti->Read(k1, read_value, t1,
                       std::bind(&TxIndexImplTest::dummyCallback, this), deps)
                  .error_code());
    ASSERT_EQ(
        azino::TxOpStatus_Code_Ok,
        ti->WriteLock(k1, t1, std::bind(&TxIndexImplTest::dummyCallback, this),
                      deps)
            .error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_ReadNotExist,
              ti->Read(k1, read_value, t1,
                       std::bind(&TxIndexImplTest::dummyCallback, this), deps)
                  .error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k1, v1, t1, deps).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_ReadNotExist,
              ti->Read(k1, read_value, t1,
                       std::bind(&TxIndexImplTest::dummyCallback, this), deps)
                  .error_code());
    t1.set_commit_ts(4);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_ReadNotExist,
              ti->Read(k1, read_value, t1,
                       std::bind(&TxIndexImplTest::dummyCallback, this), deps)
                  .error_code());
    azino::TxIdentifier read_tx_3;
    read_tx_3.set_start_ts(3);
    ASSERT_EQ(azino::TxOpStatus_Code_ReadNotExist,
              ti->Read(k1, read_value, read_tx_3,
                       std::bind(&TxIndexImplTest::dummyCallback, this), deps)
                  .error_code());
    azino::TxIdentifier read_tx_6;
    read_tx_6.set_start_ts(6);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->Read(k1, read_value, read_tx_6,
                       std::bind(&TxIndexImplTest::dummyCallback, this), deps)
                  .error_code());
    ASSERT_EQ(v1.content(), read_value.content());
}

TEST_F(TxIndexImplTest, persist) {
    std::vector<azino::txindex::Dep> deps;
    std::vector<azino::txindex::DataToPersist> datas;
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k1, v1, t1, deps).error_code());
    ASSERT_EQ(ti->GetPersisting(datas).error_code(),
              azino::TxOpStatus_Code_NoneToPersist);
    t1.set_commit_ts(3);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t1).error_code());
    t2.set_start_ts(4);
    ASSERT_EQ(
        azino::TxOpStatus_Code_Ok,
        ti->WriteLock(k1, t2, std::bind(&TxIndexImplTest::dummyCallback, this),
                      deps)
            .error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k1, v2, t2, deps).error_code());
    t2.set_commit_ts(5);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t2).error_code());
    azino::Value read_value;
    azino::TxIdentifier read_tx_3;
    read_tx_3.set_start_ts(3);
    azino::TxIdentifier read_tx_6;
    read_tx_6.set_start_ts(6);
    ASSERT_EQ(ti->Read(k1, read_value, read_tx_3, NULL, deps).error_code(),
              azino::TxOpStatus_Code_Ok);
    ASSERT_EQ(v1.content(), read_value.content());
    ASSERT_EQ(ti->Read(k1, read_value, read_tx_6, NULL, deps).error_code(),
              azino::TxOpStatus_Code_Ok);
    ASSERT_EQ(v2.content(), read_value.content());
    ASSERT_EQ(ti->GetPersisting(datas).error_code(), azino::TxOpStatus_Code_Ok);
    ASSERT_EQ(datas.size(), 1);
    ASSERT_EQ(datas[0].t2vs.size(), 2);
    ASSERT_EQ(ti->ClearPersisted(datas).error_code(),
              azino::TxOpStatus_Code_Ok);
    ASSERT_EQ(ti->ClearPersisted(datas).error_code(),
              azino::TxOpStatus_Code_ClearRepeat);
    datas.clear();
    ASSERT_EQ(ti->GetPersisting(datas).error_code(),
              azino::TxOpStatus_Code_NoneToPersist);
    ASSERT_EQ(datas.size(), 0);
    ASSERT_EQ(ti->Read(k1, read_value, read_tx_3, NULL, deps).error_code(),
              azino::TxOpStatus_Code_ReadNotExist);
    ASSERT_EQ(ti->Read(k1, read_value, read_tx_6, NULL, deps).error_code(),
              azino::TxOpStatus_Code_ReadNotExist);
}

TEST_F(TxIndexImplTest, read_dep_report) {
    std::vector<azino::txindex::Dep> deps;
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k1, v1, t1, deps).error_code());
    t1.set_commit_ts(3);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t1).error_code());

    t2.set_start_ts(5);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k1, v2, t2, deps).error_code());
    t2.set_commit_ts(6);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t2).error_code());

    azino::TxIdentifier write_tx_7;
    write_tx_7.set_start_ts(7);
    ASSERT_EQ(
        azino::TxOpStatus_Code_Ok,
        ti->WriteLock(k1, write_tx_7,
                      std::bind(&TxIndexImplTest::dummyCallback, this), deps)
            .error_code());

    azino::TxIdentifier read_tx_4;
    read_tx_4.set_start_ts(4);
    azino::Value read_value;
    deps.clear();
    ASSERT_EQ(ti->Read(k1, read_value, read_tx_4, NULL, deps).error_code(),
              azino::TxOpStatus_Code_Ok);
    ASSERT_EQ(v1.content(), read_value.content());

    ASSERT_EQ(3, deps.size());
    ASSERT_EQ(read_tx_4.start_ts(), deps[0].ts1);
    ASSERT_EQ(write_tx_7.start_ts(), deps[0].ts2);
    ASSERT_EQ(azino::txindex::DepType::READWRITE, deps[0].type);
    ASSERT_EQ(read_tx_4.start_ts(), deps[1].ts1);
    ASSERT_EQ(t2.commit_ts(), deps[1].ts2);
    ASSERT_EQ(azino::txindex::DepType::READWRITE, deps[1].type);
    ASSERT_EQ(t1.commit_ts(), deps[2].ts1);
    ASSERT_EQ(read_tx_4.start_ts(), deps[2].ts2);
    ASSERT_EQ(azino::txindex::DepType::WRITEREAD, deps[2].type);
}

TEST_F(TxIndexImplTest, write_dep_report) {
    std::vector<azino::txindex::Dep> deps;
    azino::Value read_value;
    ASSERT_EQ(ti->Read(k1, read_value, t1, NULL, deps).error_code(),
              azino::TxOpStatus_Code_ReadNotExist);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k1, v1, t1, deps).error_code());

    ASSERT_EQ(1, deps.size());
    ASSERT_EQ(t1.start_ts(), deps[0].ts1);
    ASSERT_EQ(t1.start_ts(), deps[0].ts2);
    ASSERT_EQ(azino::txindex::DepType::READWRITE, deps[0].type);

    t1.set_commit_ts(3);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t1).error_code());

    azino::TxIdentifier read_tx_4;
    read_tx_4.set_start_ts(4);
    ASSERT_EQ(ti->Read(k1, read_value, read_tx_4, NULL, deps).error_code(),
              azino::TxOpStatus_Code_Ok);
    ASSERT_EQ(v1.content(), read_value.content());

    t2.set_start_ts(5);
    deps.clear();
    ASSERT_EQ(
        azino::TxOpStatus_Code_Ok,
        ti->WriteLock(k1, t2, std::bind(&TxIndexImplTest::dummyCallback, this),
                      deps)
            .error_code());

    ASSERT_EQ(2, deps.size());
    ASSERT_EQ(t1.commit_ts(), deps[0].ts1);
    ASSERT_EQ(t2.start_ts(), deps[0].ts2);
    ASSERT_EQ(azino::txindex::DepType::WRITEWRITE, deps[0].type);
    ASSERT_EQ(read_tx_4.start_ts(), deps[1].ts1);
    ASSERT_EQ(t2.start_ts(), deps[1].ts2);
    ASSERT_EQ(azino::txindex::DepType::READWRITE, deps[1].type);

    deps.clear();
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k1, v2, t2, deps).error_code());

    ASSERT_EQ(2, deps.size());
    ASSERT_EQ(t1.commit_ts(), deps[0].ts1);
    ASSERT_EQ(t2.start_ts(), deps[0].ts2);
    ASSERT_EQ(azino::txindex::DepType::WRITEWRITE, deps[0].type);
    ASSERT_EQ(read_tx_4.start_ts(), deps[1].ts1);
    ASSERT_EQ(t2.start_ts(), deps[1].ts2);
    ASSERT_EQ(azino::txindex::DepType::READWRITE, deps[1].type);

    t2.set_commit_ts(6);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t2).error_code());

    azino::TxIdentifier write_tx_7;
    write_tx_7.set_start_ts(7);
    deps.clear();
    ASSERT_EQ(azino::TxOpStatus_Code_Ok,
              ti->WriteIntent(k1, v1, write_tx_7, deps).error_code());

    ASSERT_EQ(1, deps.size());
    ASSERT_EQ(t2.commit_ts(), deps[0].ts1);
    ASSERT_EQ(write_tx_7.start_ts(), deps[0].ts2);
    ASSERT_EQ(azino::txindex::DepType::WRITEWRITE, deps[0].type);
}