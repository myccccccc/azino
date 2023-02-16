#ifndef AZINO_INCLUDE_COMPARATOR_H
#define AZINO_INCLUDE_COMPARATOR_H

#include <algorithm>
#include <cstring>
#include <string>

namespace azino {
class BitWiseComparator {
   public:
    inline bool operator()(const std::string& lhs,
                           const std::string& rhs) const {
        const size_t min_len = std::min(lhs.size(), rhs.size());
        int r = memcmp(lhs.data(), rhs.data(), min_len);
        if (r == 0) {
            if (lhs.size() < rhs.size())
                r = -1;
            else if (lhs.size() > rhs.size())
                r = +1;
        }
        return r < 0;
    }
};
}  // namespace azino

#endif  // AZINO_INCLUDE_COMPARATOR_H
