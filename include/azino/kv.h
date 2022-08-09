#ifndef AZINO_INCLUDE_KV_H
#define AZINO_INCLUDE_KV_H

#include <string>

namespace azino {
typedef uint64_t TimeStamp;
#define MAX_TIMESTAMP UINT64_MAX
#define MIN_TIMESTAMP 0
typedef std::string UserKey;
typedef std::string UserValue;
}  // namespace azino

#endif  // AZINO_INCLUDE_KV_H
