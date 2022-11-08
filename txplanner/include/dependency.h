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
    virtual uint64_t fromID() const = 0;
    virtual uint64_t toID() const = 0;
};
typedef std::shared_ptr<Dependence> DependencePtr;

class DependenceHash {
   public:
    std::size_t operator()(const DependencePtr& d) const {
        return std::hash<uint64_t>()(d->fromID() + d->toID());
    }
};

class DependenceEqual {
   public:
    bool operator()(const DependencePtr& d1, const DependencePtr& d2) const {
        return d1->Type() == d2->Type() && d1->fromID() == d2->fromID() &&
               d1->toID() == d2->toID();
    }
};

typedef std::unordered_set<DependencePtr, DependenceHash, DependenceEqual>
    DependenceSet;
typedef std::unordered_set<uint64_t> IDSet;

class DependenceGraph {
   public:
    virtual DependenceSet GetInDependence(uint64_t id) = 0;
    virtual DependenceSet GetOutDependence(uint64_t id) = 0;
    virtual IDSet ListID() = 0;
};

typedef std::vector<DependencePtr> DependenceCycle;
class DependenceChecker {
   public:
    virtual DependenceCycle Check(const DependenceGraph*) = 0;
};

}  // namespace txplanner
}  // namespace azino
#endif  // AZINO_TXPLANNER_INCLUDE_DEPENDENCY_H
