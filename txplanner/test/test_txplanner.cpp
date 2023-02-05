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
    TxIdentifier tx_1_3;
    tx_1_3.set_start_ts(1);
    tx_1_3.set_commit_ts(3);
    TxIdentifier tx_2_4;
    tx_2_4.set_start_ts(2);
    tx_2_4.set_commit_ts(4);
    TxIdentifier tx_5_6;
    tx_5_6.set_start_ts(5);
    tx_5_6.set_commit_ts(6);
    table->UpsertTxID(tx_1_3);
    table->UpsertTxID(tx_2_4);
    table->UpsertTxID(tx_5_6);
    ASSERT_EQ(0, table->AddDep(azino::txplanner::READWRITE, 1, 2));
    ASSERT_EQ(0, table->AddDep(azino::txplanner::READWRITE, 2, 5));

    auto point_set = table->List();
    ASSERT_EQ(3, point_set.size());
    std::unordered_set<uint64_t> id_set;
    for (const auto& p : point_set) {
        id_set.insert(p->ID());
    }
    ASSERT_TRUE(id_set.find(1) != id_set.end());
    ASSERT_TRUE(id_set.find(2) != id_set.end());
    ASSERT_TRUE(id_set.find(5) != id_set.end());

    auto dep_set = table->GetOutDependence(1);
    ASSERT_EQ(1, dep_set.size());
    ASSERT_EQ(2, (*(dep_set.begin()))->ID());

    dep_set = table->GetInDependence(4);
    ASSERT_EQ(1, dep_set.size());
    ASSERT_EQ(1, (*(dep_set.begin()))->ID());

    dep_set = table->GetOutDependence(2);
    ASSERT_EQ(1, dep_set.size());
    ASSERT_EQ(5, (*(dep_set.begin()))->ID());

    dep_set = table->GetInDependence(5);
    ASSERT_EQ(1, dep_set.size());
    ASSERT_EQ(2, (*(dep_set.begin()))->ID());

    // test delete txid
    ASSERT_EQ(0, table->DeleteTxID(tx_2_4));

    point_set = table->List();
    ASSERT_EQ(2, point_set.size());

    id_set.clear();
    for (const auto& p : point_set) {
        id_set.insert(p->ID());
    }
    ASSERT_TRUE(id_set.find(1) != id_set.end());
    ASSERT_TRUE(id_set.find(5) != id_set.end());

    dep_set = table->GetOutDependence(1);
    ASSERT_EQ(0, dep_set.size());

    dep_set = table->GetInDependence(5);
    ASSERT_EQ(0, dep_set.size());
}