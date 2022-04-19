#include "persistor.h"
#include <gflags/gflags.h>


DEFINE_int32(persist_period, 10000, "Persist period time. Measurement: millisecond.");
DEFINE_string(storage_addr, "0.0.0.0:8000", "Address of storage");
DEFINE_bool(enable_persistor, true, "If enable persistor to persist data to storage server.");