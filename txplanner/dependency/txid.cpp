#include "txid.h"

std::hash<uint64_t> hash;

namespace azino {
namespace txplanner {
bool TxIDPtrEqual::operator()(const TxIDPtr& c1, const TxIDPtr& c2) const {
    return c1->ID() == c2->ID();
}

std::size_t TxIDPtrHash::operator()(const TxIDPtr& c) const {
    return hash(c->ID());
}
}  // namespace txplanner
}  // namespace azino