#ifndef AZINO_INCLUDE_RANGE_H
#define AZINO_INCLUDE_RANGE_H

#include <sstream>

#include "comparator.h"
#include "service/partition.pb.h"

namespace azino {
class RangeComparator;
class Range {
   public:
    Range(const std::string l, const std::string r, int li, int ri)
        : left(l), right(r), left_include(li), right_include(ri) {}
    inline bool Contains(const Range& rg) const {
        bool left_contain = false;
        bool right_contain = false;
        if (left == "" || (rg.left != "" && cmp(left, rg.left)) ||
            (left == rg.left && left_include >= rg.left_include)) {
            left_contain = true;
        }
        if (right == "" || (rg.right != "" && cmp(rg.right, right)) ||
            (right == rg.right && right_include >= rg.right_include)) {
            right_contain = true;
        }
        return left_contain && right_contain;
    }
    std::string Describe() const {
        std::stringstream ss;
        ss << (left_include > 0 ? "[" : "(");
        ss << left << ", " << right;
        ss << (right_include > 0 ? "]" : ")");
        return ss.str();
    }

    RangePB ToPB() const {
        RangePB pb;
        pb.set_left(left);
        pb.set_right(right);
        pb.set_left_include(left_include);
        pb.set_right_include(right_include);
        return pb;
    }

    static Range FromPB(const RangePB& pb) {
        return Range(pb.left(), pb.right(), pb.left_include(),
                     pb.right_include());
    }

   private:
    friend class RangeComparator;
    BitWiseComparator cmp;
    const std::string left;   // "" is negative unlimited
    const std::string right;  // "" is positive unlimited
    const int left_include;   // > 0 means include
    const int right_include;
};

class RangeComparator {
   public:
    // return true when range lhs is completely before rhs
    inline bool operator()(const Range& lhs, const Range& rhs) {
        if (lhs.right == "" || rhs.left == "") {
            return false;
        }
        if (cmp(lhs.right, rhs.left)) {
            return true;
        } else if (lhs.right == rhs.left &&
                   !(lhs.right_include > 0 && rhs.left_include > 0)) {
            return true;
        }
        return false;
    }

   private:
    BitWiseComparator cmp;
};

typedef std::set<Range, RangeComparator> RangeSet;
}  // namespace azino
#endif  // AZINO_INCLUDE_RANGE_H
