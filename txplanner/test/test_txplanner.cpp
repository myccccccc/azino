#include <gtest/gtest.h>

#include "dependency.h"
#include "txidtable.h"

using namespace azino::txplanner;
using namespace azino;

class DependencyTest : public testing::Test {
   public:
    DependencyTest() {}

   protected:
    void SetUp() {
        table = new TxIDTable();
        graph = table;
    }
    void TearDown() {
        delete table;
        graph = nullptr;
    }

   public:
    DependenceGraph* graph;
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
    ASSERT_EQ(0, table->AddDep(azino::txplanner::WRITEREAD, 5, 1));
    ASSERT_EQ(0, table->AddDep(azino::txplanner::WRITEREAD, 6, 1));

    auto point_set = graph->List();
    ASSERT_EQ(3, point_set.size());
    std::unordered_set<uint64_t> id_set;
    for (const auto& p : point_set) {
        id_set.insert(p->ID());
    }
    ASSERT_TRUE(id_set.find(1) != id_set.end());
    ASSERT_TRUE(id_set.find(2) != id_set.end());
    ASSERT_TRUE(id_set.find(5) != id_set.end());

    auto dep_set = graph->GetInDependence(1);
    ASSERT_EQ(1, dep_set.size());
    ASSERT_EQ(azino::txplanner::WRITEREAD, (*(dep_set.begin()))->Type());
    ASSERT_EQ(5, (*(dep_set.begin()))->fromPoint()->ID());
    ASSERT_EQ(1, (*(dep_set.begin()))->toPoint()->ID());

    dep_set = graph->GetOutDependence(1);
    ASSERT_EQ(1, dep_set.size());
    ASSERT_EQ(azino::txplanner::READWRITE, (*(dep_set.begin()))->Type());
    ASSERT_EQ(2, (*(dep_set.begin()))->toPoint()->ID());
    ASSERT_EQ(1, (*(dep_set.begin()))->fromPoint()->ID());

    dep_set = graph->GetInDependence(4);
    ASSERT_EQ(1, dep_set.size());
    ASSERT_EQ(azino::txplanner::READWRITE, (*(dep_set.begin()))->Type());
    ASSERT_EQ(1, (*(dep_set.begin()))->fromPoint()->ID());
    ASSERT_EQ(2, (*(dep_set.begin()))->toPoint()->ID());

    dep_set = graph->GetOutDependence(2);
    ASSERT_EQ(1, dep_set.size());
    ASSERT_EQ(azino::txplanner::READWRITE, (*(dep_set.begin()))->Type());
    ASSERT_EQ(5, (*(dep_set.begin()))->toPoint()->ID());
    ASSERT_EQ(2, (*(dep_set.begin()))->fromPoint()->ID());

    dep_set = graph->GetInDependence(5);
    ASSERT_EQ(1, dep_set.size());
    ASSERT_EQ(azino::txplanner::READWRITE, (*(dep_set.begin()))->Type());
    ASSERT_EQ(2, (*(dep_set.begin()))->fromPoint()->ID());
    ASSERT_EQ(5, (*(dep_set.begin()))->toPoint()->ID());

    dep_set = graph->GetOutDependence(5);
    ASSERT_EQ(1, dep_set.size());
    ASSERT_EQ(azino::txplanner::WRITEREAD, (*(dep_set.begin()))->Type());
    ASSERT_EQ(1, (*(dep_set.begin()))->toPoint()->ID());
    ASSERT_EQ(5, (*(dep_set.begin()))->fromPoint()->ID());

    // test delete dependency
    dep_set = graph->GetOutDependence(5);
    ASSERT_EQ(0, table->DelDep(*dep_set.begin()));
    dep_set = graph->GetOutDependence(5);
    ASSERT_EQ(0, dep_set.size());
    dep_set = graph->GetInDependence(1);
    ASSERT_EQ(0, dep_set.size());

    // test delete txid
    ASSERT_EQ(0, table->DeleteTxID(tx_5_6));

    point_set = graph->List();
    ASSERT_EQ(2, point_set.size());
    id_set.clear();
    for (const auto& p : point_set) {
        id_set.insert(p->ID());
    }
    ASSERT_TRUE(id_set.find(1) != id_set.end());
    ASSERT_TRUE(id_set.find(2) != id_set.end());

    dep_set = graph->GetInDependence(1);
    ASSERT_EQ(0, dep_set.size());

    dep_set = graph->GetOutDependence(1);
    ASSERT_EQ(1, dep_set.size());
    ASSERT_EQ(azino::txplanner::READWRITE, (*(dep_set.begin()))->Type());
    ASSERT_EQ(2, (*(dep_set.begin()))->toPoint()->ID());
    ASSERT_EQ(1, (*(dep_set.begin()))->fromPoint()->ID());

    dep_set = graph->GetInDependence(4);
    ASSERT_EQ(1, dep_set.size());
    ASSERT_EQ(azino::txplanner::READWRITE, (*(dep_set.begin()))->Type());
    ASSERT_EQ(1, (*(dep_set.begin()))->fromPoint()->ID());
    ASSERT_EQ(2, (*(dep_set.begin()))->toPoint()->ID());

    dep_set = graph->GetOutDependence(2);
    ASSERT_EQ(0, dep_set.size());

    dep_set = graph->GetInDependence(5);
    ASSERT_EQ(0, dep_set.size());

    dep_set = graph->GetOutDependence(5);
    ASSERT_EQ(0, dep_set.size());

    ASSERT_EQ(ENOENT, table->DeleteTxID(tx_5_6));
}