#include <gflags/gflags.h>

#include <iostream>

#include "azino/client.h"

DEFINE_string(txplanner_addr, "0.0.0.0:8001", "Address of txplanner");

int main(int argc, char* argv[]) {
    // Parse gflags. We recommend you to use gflags as well.
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    azino::Options options{FLAGS_txplanner_addr};
    azino::Transaction tx(options);

    while (true) {
        while (true) {
            std::string action;
            std::string key, right_key, value;
            std::vector<std::string> keys;
            std::vector<std::string> values;
            std::cin >> action;
            if (action == "begin") {
                auto sts = tx.Begin();
                std::cout << sts.ToString() << std::endl;
            } else if (action == "commit") {
                auto sts = tx.Commit();
                std::cout << sts.ToString() << std::endl;
                break;
            } else if (action == "abort") {
                auto sts = tx.Abort();
                std::cout << sts.ToString() << std::endl;
                break;
            } else if (action == "put") {
                std::cin >> key >> value;
                azino::WriteOptions opts;
                opts.type = azino::kAutomatic;
                auto sts = tx.Put(opts, key, value);
                std::cout << sts.ToString() << std::endl;
            } else if (action == "pput") {
                std::cin >> key >> value;
                azino::WriteOptions opts;
                opts.type = azino::kPessimistic;
                auto sts = tx.Put(opts, key, value);
                std::cout << sts.ToString() << std::endl;
            } else if (action == "oput") {
                std::cin >> key >> value;
                azino::WriteOptions opts;
                opts.type = azino::kOptimistic;
                auto sts = tx.Put(opts, key, value);
                std::cout << sts.ToString() << std::endl;
            } else if (action == "get") {
                std::cin >> key;
                azino::ReadOptions opts;
                auto sts = tx.Get(opts, key, value);
                std::cout << sts.ToString() << std::endl;
                std::cout << value << std::endl;
            } else if (action == "delete") {
                std::cin >> key;
                azino::WriteOptions opts;
                opts.type = azino::kAutomatic;
                auto sts = tx.Delete(opts, key);
                std::cout << sts.ToString() << std::endl;
            } else if (action == "pdelete") {
                std::cin >> key;
                azino::WriteOptions opts;
                opts.type = azino::kPessimistic;
                auto sts = tx.Delete(opts, key);
                std::cout << sts.ToString() << std::endl;
            } else if (action == "odelete") {
                std::cin >> key;
                azino::WriteOptions opts;
                opts.type = azino::kOptimistic;
                auto sts = tx.Delete(opts, key);
                std::cout << sts.ToString() << std::endl;
            } else if (action == "scan") {
                std::cin >> key;
                std::cin >> right_key;
                auto sts = tx.Scan(key, right_key, keys, values);
                std::cout << sts.ToString() << std::endl;
                for (size_t i = 0; i < keys.size(); i++) {
                    std::cout << keys[i] << " " << values[i] << std::endl;
                }
            } else {
                std::getline(std::cin, action);
                std::cout
                    << "Use put, pput, oput, get or delete, pdelete, odelete"
                    << std::endl;
                continue;
            }
        }
        tx.Reset();
    }
}
