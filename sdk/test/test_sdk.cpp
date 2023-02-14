#include <gtest/gtest.h>

#include "azino/comparator.h"

class SDKTest : public testing::Test {
   public:
   protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(SDKTest, comparator) {
    std::set<std::string, azino::BitWiseComparator> m;
    m.insert("a");
    m.insert("b");
    m.insert("a1");
    m.insert("");
    auto iter = m.begin();
    ASSERT_EQ("", *iter);
    iter++;
    ASSERT_EQ("a", *iter);
    iter++;
    ASSERT_EQ("a1", *iter);
    iter++;
    ASSERT_EQ("b", *iter);
    iter++;
}