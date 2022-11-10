#ifndef AZINO_TXPLANNER_INCLUDE_POINT_H
#define AZINO_TXPLANNER_INCLUDE_POINT_H

#include <memory>
#include <unordered_set>

#include "dependency.h"

namespace azino {
namespace txplanner {

class Point {
   public:
    virtual uint64_t ID() const = 0;
    virtual DependenceSet GetInDependence() const = 0;
    virtual DependenceSet GetOutDependence() const = 0;
};

}  // namespace txplanner
}  // namespace azino

#endif  // AZINO_TXPLANNER_INCLUDE_POINT_H
