#ifndef AZINO_TXPLANNER_INCLUDE_DEPENDENCY_H
#define AZINO_TXPLANNER_INCLUDE_DEPENDENCY_H

#include <unordered_set>
#include <vector>

#include "azino/kv.h"

namespace azino {
namespace txplanner {

class Point;
typedef std::shared_ptr<Point> PointPtr;
class PointHash {
   public:
    std::size_t operator()(const PointPtr& p) const;
};
class PointEqual {
   public:
    bool operator()(const PointPtr& p1, const PointPtr& p2) const;
};
typedef std::unordered_set<PointPtr, PointHash, PointEqual> PointSet;

enum DepType { READWRITE = 1, WRITEWRITE = 2, WRITEREAD = 3 };
class Dependence {
   public:
    virtual DepType Type() const = 0;
    virtual PointPtr fromPoint() const = 0;
    virtual PointPtr toPoint() const = 0;
};
typedef std::shared_ptr<Dependence> DependencePtr;
class DependenceHash {
   public:
    std::size_t operator()(const DependencePtr& d) const;
};
class DependenceEqual {
   public:
    bool operator()(const DependencePtr& d1, const DependencePtr& d2) const;
};
typedef std::unordered_set<DependencePtr, DependenceHash, DependenceEqual>
    DependenceSet;
typedef std::vector<DependencePtr> DependenceCycle;

class DependenceGraph {
   public:
    virtual DependenceSet GetInDependence(uint64_t id) = 0;
    virtual DependenceSet GetOutDependence(uint64_t id) = 0;
    virtual PointSet List() = 0;
};

class DependenceChecker {
   public:
    virtual DependenceCycle Check(const DependenceGraph*) = 0;
};

}  // namespace txplanner
}  // namespace azino
#endif  // AZINO_TXPLANNER_INCLUDE_DEPENDENCY_H
