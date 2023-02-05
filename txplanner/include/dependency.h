#ifndef AZINO_TXPLANNER_INCLUDE_DEPENDENCY_H
#define AZINO_TXPLANNER_INCLUDE_DEPENDENCY_H

#include <unordered_set>
#include <vector>

#include "azino/kv.h"

namespace azino {
namespace txplanner {
enum DepType { READWRITE = 1 };

}  // namespace txplanner
}  // namespace azino
#endif  // AZINO_TXPLANNER_INCLUDE_DEPENDENCY_H
