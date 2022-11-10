#include "point.h"

namespace azino {
namespace txplanner {
std::size_t DependenceHash::operator()(const DependencePtr& d) const {
    return std::hash<uint64_t>()(d->fromPoint()->ID() + d->toPoint()->ID());
}

bool DependenceEqual::operator()(const DependencePtr& d1,
                                 const DependencePtr& d2) const {
    return d1->Type() == d2->Type() &&
           d1->fromPoint()->ID() == d2->fromPoint()->ID() &&
           d1->toPoint()->ID() == d2->toPoint()->ID();
}

std::size_t PointHash::operator()(const PointPtr& p) const {
    return std::hash<uint64_t>()(p->ID());
}

bool PointEqual::operator()(const PointPtr& p1, const PointPtr& p2) const {
    return p1->ID() == p2->ID();
}
}  // namespace txplanner
}  // namespace azino
