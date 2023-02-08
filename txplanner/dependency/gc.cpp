#include "gc.h"

#include <gflags/gflags.h>

#include "txidtable.h"

DEFINE_int32(gc_period_ms, 1000, "gc period time");

namespace azino {
namespace txplanner {
GC::GC(TxIDTable *table) : _table(table) {}

void *GC::execute(void *args) {
    auto p = reinterpret_cast<GC *>(args);
    while (true) {
        bthread_usleep(FLAGS_gc_period_ms * 1000);
        {
            std::lock_guard<bthread::Mutex> lck(p->_mutex);
            if (p->_stopped) {
                break;
            }
        }
        p->_table->GCTx();
    }
    return nullptr;
}
}  // namespace txplanner
}  // namespace azino