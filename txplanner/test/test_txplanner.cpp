#include <gtest/gtest.h>

#include "dependency.h"
#include "txidtable.h"

using namespace azino::txplanner;
using namespace azino;

class DependencyTest : public testing::Test {
   public:
    DependencyTest() {}

   protected:
    void SetUp() { table = new TxIDTable(); }
    void TearDown() { delete table; }

   public:
    TxIDTable* table;
};

TEST_F(DependencyTest, graph_basic) {
    auto tx_1 = table->BeginTx(1);
    auto tx_2 = table->BeginTx(2);
    auto tx_5 = table->BeginTx(5);
    table->AddDep(azino::txplanner::READWRITE, tx_1->get_txid(),
                  tx_2->get_txid());
    table->AddDep(azino::txplanner::READWRITE, tx_2->get_txid(),
                  tx_5->get_txid());

    auto point_set = table->List();
    ASSERT_EQ(3, point_set.size());
    std::unordered_set<uint64_t> id_set;
    for (const auto& p : point_set) {
        id_set.insert(p->start_ts());
    }
    ASSERT_TRUE(id_set.find(1) != id_set.end());
    ASSERT_TRUE(id_set.find(2) != id_set.end());
    ASSERT_TRUE(id_set.find(5) != id_set.end());

    auto dep_set = tx_1->get_out();
    ASSERT_EQ(1, dep_set.size());
    ASSERT_EQ(2, (*(dep_set.begin()))->start_ts());

    dep_set = tx_2->get_in();
    ASSERT_EQ(1, dep_set.size());
    ASSERT_EQ(1, (*(dep_set.begin()))->start_ts());

    dep_set = tx_2->get_out();
    ASSERT_EQ(1, dep_set.size());
    ASSERT_EQ(5, (*(dep_set.begin()))->start_ts());

    dep_set = tx_5->get_in();
    ASSERT_EQ(1, dep_set.size());
    ASSERT_EQ(2, (*(dep_set.begin()))->start_ts());

    ASSERT_EQ(0, table->FindAbortTxnOnConsecutiveRWDep(tx_1).size());
    ASSERT_EQ(0, table->FindAbortTxnOnConsecutiveRWDep(tx_2).size());
    ASSERT_EQ(0, table->FindAbortTxnOnConsecutiveRWDep(tx_5).size());

    table->CommitTx(tx_5->get_txid(), 6);
    ASSERT_EQ(TxStatus_Code_Commit, tx_5->get_txid().status().status_code());
    ASSERT_EQ(0, table->FindAbortTxnOnConsecutiveRWDep(tx_1).size());
    ASSERT_EQ(0, table->FindAbortTxnOnConsecutiveRWDep(tx_2).size());
    ASSERT_EQ(0, table->FindAbortTxnOnConsecutiveRWDep(tx_5).size());

    table->CommitTx(tx_2->get_txid(), 7);
    ASSERT_EQ(TxStatus_Code_Commit, tx_2->get_txid().status().status_code());
    ASSERT_EQ(1, table->FindAbortTxnOnConsecutiveRWDep(tx_1).size());
    ASSERT_EQ(1, table->FindAbortTxnOnConsecutiveRWDep(tx_2).size());
    ASSERT_EQ(1, table->FindAbortTxnOnConsecutiveRWDep(tx_5).size());
    ASSERT_EQ(
        1,
        (*(table->FindAbortTxnOnConsecutiveRWDep(tx_1).begin()))->start_ts());

    table->AbortTx(tx_1->get_txid());
    ASSERT_EQ(TxStatus_Code_Abort, tx_1->get_txid().status().status_code());
    dep_set = tx_1->get_out();
    ASSERT_EQ(0, dep_set.size());
    dep_set = tx_2->get_in();
    ASSERT_EQ(0, dep_set.size());
}

TEST_F(DependencyTest, graph_basic2) {
    auto tx_5 = table->BeginTx(5);
    auto tx_6 = table->BeginTx(6);
    table->AddDep(azino::txplanner::READWRITE, tx_6->get_txid(),
                  tx_5->get_txid());
    table->CommitTx(tx_5->get_txid(), 7);
    table->AddDep(azino::txplanner::READWRITE, tx_5->get_txid(),
                  tx_6->get_txid());
    ASSERT_EQ(1, table->FindAbortTxnOnConsecutiveRWDep(tx_5).size());
    ASSERT_EQ(1, table->FindAbortTxnOnConsecutiveRWDep(tx_6).size());
}