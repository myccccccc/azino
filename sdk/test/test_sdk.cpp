#include <gtest/gtest.h>

#include "azino/comparator.h"
#include "azino/range.h"

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

TEST_F(SDKTest, range) {
    std::set<azino::Range, azino::RangeComparator> m;
    m.insert(azino::Range("g", "", 1, 0));
    m.insert(azino::Range("", "a", 0, 0));
    m.insert(azino::Range("c", "c", 1, 1));
    m.insert(azino::Range("a", "c", 1, 0));
    auto iter = m.begin();
    ASSERT_EQ("(, a)", iter->Describe());
    iter++;
    ASSERT_EQ("[a, c)", iter->Describe());
    iter++;
    ASSERT_EQ("[c, c]", iter->Describe());
    iter++;
    ASSERT_EQ("[g, )", iter->Describe());
    iter++;

    iter = m.lower_bound(azino::Range("a", "a", 1, 1));
    ASSERT_TRUE(iter != m.end());
    ASSERT_EQ("[a, c)", iter->Describe());
    ASSERT_TRUE(iter->Contains(azino::Range("a", "a", 1, 1)));

    iter = m.lower_bound(azino::Range("c", "c", 1, 1));
    ASSERT_TRUE(iter != m.end());
    ASSERT_EQ("[c, c]", iter->Describe());
    ASSERT_TRUE(iter->Contains(azino::Range("c", "c", 1, 1)));

    iter = m.lower_bound(azino::Range("e", "e", 1, 1));
    ASSERT_TRUE(iter != m.end());
    ASSERT_EQ("[g, )", iter->Describe());
    ASSERT_FALSE(iter->Contains(azino::Range("e", "e", 1, 1)));

    iter = m.lower_bound(azino::Range("x", "x", 1, 1));
    ASSERT_TRUE(iter != m.end());
    ASSERT_EQ("[g, )", iter->Describe());
    ASSERT_TRUE(iter->Contains(azino::Range("x", "x", 1, 1)));
}