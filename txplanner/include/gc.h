#ifndef AZINO_TXPLANNER_INCLUDE_GC_H
#define AZINO_TXPLANNER_INCLUDE_GC_H

#include "azino/background_task.h"
namespace azino {
namespace txplanner {
class TxIDTable;

class GC : public BackgroundTask {
   public:
    GC(TxIDTable* table);
    ~GC() = default;

   private:
    static void* execute(void*);
    TxIDTable* _table;
};
}  // namespace txplanner
}  // namespace azino

#endif  // AZINO_TXPLANNER_INCLUDE_GC_H
