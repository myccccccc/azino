#ifndef AZINO_INCLUDE_OPTIONS_H
#define AZINO_INCLUDE_OPTIONS_H

namespace azino {
struct Options {
    std::string txplanner_addr;
};

struct ReadOptions {};

enum WriteType { kAutomatic = 0, kOptimistic = 1, kPessimistic = 2 };

struct WriteOptions {
    WriteType type = kAutomatic;
};

}  // namespace azino
#endif  // AZINO_INCLUDE_OPTIONS_H
