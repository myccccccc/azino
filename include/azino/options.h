#ifndef AZINO_INCLUDE_OPTIONS_H
#define AZINO_INCLUDE_OPTIONS_H

namespace azino {
enum IsolationLevel {
    Snapshot = 0,
    SerializableSnapshot = 1,
    Serializable = 2
};

struct Options {
    std::string txplanner_addr;
    IsolationLevel iso = Snapshot;
};

// useful in serializable isolation level
enum ReadType { kReadOptimistic = 0, kReadPessimistic = 1 };

struct ReadOptions {};

enum WriteType { kAutomatic = 0, kOptimistic = 1, kPessimistic = 2 };

struct WriteOptions {
    WriteType type = kAutomatic;
};

}  // namespace azino
#endif  // AZINO_INCLUDE_OPTIONS_H
