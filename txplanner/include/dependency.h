#ifndef AZINO_TXPLANNER_INCLUDE_DEPENDENCY_H
#define AZINO_TXPLANNER_INCLUDE_DEPENDENCY_H

#include <unordered_set>

#include "azino/kv.h"

namespace azino {
namespace txplanner {

enum DepType { READWRITE = 1, WRITEWRITE = 2, WRITEREAD = 3 };

class Dependence {
   public:
    virtual DepType Type() const = 0;
    virtual uint64_t ID() const = 0;
};

class DependenceHash {
   public:
    std::size_t operator()(const Dependence* d) const {
        return std::hash<uint64_t>()(d->ID());
    }
};

class DependenceEqual {
   public:
    bool operator()(const Dependence* d1, const Dependence* d2) const {
        return d1->Type() == d2->Type() && d1->ID() == d2->ID();
    }
};

typedef std::unordered_set<Dependence*, DependenceHash, DependenceEqual>
    DependenceSet;
typedef std::unordered_set<uint64_t> IDSet;

class DependenceGraph {
   public:
    virtual DependenceSet GetInDependence(uint64_t id) = 0;
    virtual DependenceSet GetOutDependence(uint64_t id) = 0;
    virtual IDSet ListID() = 0;
};

class DependenceChecker {};

}  // namespace txplanner
}  // namespace azino
#endif  // AZINO_TXPLANNER_INCLUDE_DEPENDENCY_H
