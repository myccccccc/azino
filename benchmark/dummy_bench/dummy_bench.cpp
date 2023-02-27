#include <butil/fast_rand.h>
#include <butil/logging.h>
#include <gflags/gflags.h>

#include <iostream>

#include "azino/client.h"

DEFINE_string(txplanner_addr, "0.0.0.0:8001", "Address of txplanner");
DEFINE_int32(round_num, 1000, "execution round number");
DEFINE_int32(op_num, 5, "operation number per round");
DEFINE_int32(kv_len, 16, "key value length in every operation");
DEFINE_string(log_file, "log_dummy_bench", "log file name for dummy_bench");
namespace logging {
DECLARE_bool(crash_on_fatal_log);
}

int main(int argc, char* argv[]) {
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    logging::FLAGS_crash_on_fatal_log = true;
    logging::LoggingSettings log_settings;
    log_settings.logging_dest = logging::LoggingDestination::LOG_TO_FILE;
    log_settings.log_file = FLAGS_log_file.c_str();
    log_settings.delete_old =
        logging::OldFileDeletionState::DELETE_OLD_LOG_FILE;
    logging::InitLogging(log_settings);

    azino::Options options;
    azino::Transaction tx(options, FLAGS_txplanner_addr);

    int i = 0;
    while (i++ < FLAGS_round_num) {
        {
            // begin
            auto sts = azino::Status::Ok();
            sts = tx.Begin();
            std::cout << sts.ToString() << std::endl;
        }
        {
            // do operations
            int j = 0;
            while (j++ < FLAGS_op_num) {
                std::string key, value;
                azino::WriteOptions opts;
                azino::ReadOptions read_opts;
                auto sts = azino::Status::Ok();
                auto op = butil::fast_rand_less_than(3);
                switch (op) {
                    case 0: {
                        key = butil::fast_rand_printable(FLAGS_kv_len);
                        value = butil::fast_rand_printable(FLAGS_kv_len);
                        opts.type = azino::kAutomatic;
                        sts = tx.Put(opts, key, value);
                        std::cout << "Put key:" << key << " value:" << value
                                  << " status:" << sts.ToString();
                        break;
                    }
                    case 1: {
                        key = butil::fast_rand_printable(FLAGS_kv_len);
                        opts.type = azino::kAutomatic;
                        sts = tx.Delete(opts, key);
                        std::cout << "Delete key:" << key
                                  << " status:" << sts.ToString();
                        break;
                    }
                    case 2: {
                        key = butil::fast_rand_printable(FLAGS_kv_len);
                        sts = tx.Get(read_opts, key, value);
                        std::cout << "Get key:" << key << " value:" << value
                                  << " status:" << sts.ToString();
                        break;
                    }
                    default: {
                        assert(0);
                    }
                }
            }
        }
        {
            // commit or abort
            auto sts = azino::Status::Ok();
            auto op = butil::fast_rand_less_than(2);
            switch (op) {
                case 0: {
                    sts = tx.Commit();
                    break;
                }
                case 1: {
                    sts = tx.Abort();
                    break;
                }
                default: {
                    assert(0);
                }
            }
            std::cout << sts.ToString() << std::endl;
        }
        tx.Reset();
    }
}
