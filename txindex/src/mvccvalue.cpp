#include "mvccvalue.h"

#include "bthread/bthread.h"

extern "C" void* CallbackWrapper(void* arg) {
    auto* func = reinterpret_cast<std::function<void()>*>(arg);
    func->operator()();
    delete func;
    return nullptr;
}

DECLARE_int32(region_metric_period_s);
DEFINE_double(alpha, 1,
              "alpha hyper parameter when calculating keyPessimismDegree");
static bvar::GFlag gflag_alpha("alpha");
DEFINE_double(lambda, 0.6, "lambda hyper parameter");
static bvar::GFlag gflag_lambda("lambda");

namespace azino {
namespace txindex {

void MVCCValue::Lock(const TxIdentifier& txid) {
    _lock = MVCCLock::WriteLock;
    _lock_holder.CopyFrom(txid);
}
void MVCCValue::Prewrite(const Value& v, const TxIdentifier& txid) {
    _lock = MVCCLock::WriteIntent;
    _lock_holder.CopyFrom(txid);
    _lock_value.reset(new Value(v));
}
void MVCCValue::Clean() {
    _lock = MVCCLock::None;
    _lock_holder.Clear();
    _lock_value.reset();
}
void MVCCValue::Commit(const TxIdentifier& txid) {
    _lock = MVCCLock::None;
    _lock_holder.Clear();
    _mvv.insert(std::make_pair(txid, std::move(_lock_value)));
}

MultiVersionValue::const_iterator MVCCValue::LargestTSValue() const {
    return _mvv.begin();
}

MultiVersionValue::const_iterator MVCCValue::Seek(TimeStamp ts) const {
    TxIdentifier tmp_tx;
    tmp_tx.set_commit_ts(ts);
    return _mvv.lower_bound(tmp_tx);
}

MultiVersionValue::const_iterator MVCCValue::Seek2(TimeStamp ts) const {
    TxIdentifier tmp_tx;
    tmp_tx.set_commit_ts(ts);
    return _mvv.upper_bound(tmp_tx);
}

unsigned MVCCValue::Truncate(const TxIdentifier& txid) {
    auto iter = _mvv.lower_bound(txid);
    auto ans = _mvv.size();
    _mvv.erase(iter, _mvv.end());
    return ans - _mvv.size();
}

void MVCCValue::WakeUpWaiters() {
    for (auto& func : _waiters) {
        bthread_t bid;
        auto* arg = new std::function<void()>(func);
        if (bthread_start_background(&bid, nullptr, CallbackWrapper, arg) !=
            0) {
            LOG(ERROR) << "Failed to start callback.";
        }
    }
    _waiters.clear();
}

MVCCValue::MVCCValue()
    : _lock(MVCCLock::None),
      _lock_holder(),
      _lock_value(),
      _mvv(),
      _write(),
      _write_window(&_write, FLAGS_region_metric_period_s),
      _write_error(),
      _write_error_window(&_write_error, FLAGS_region_metric_period_s),
      _tx_op_num(),
      _tx_op_num_window(&_tx_op_num, FLAGS_region_metric_period_s),
      _tx_op_after_write_num(),
      _tx_op_after_write_num_window(&_tx_op_after_write_num,
                                    FLAGS_region_metric_period_s) {}

void MVCCValue::RecordWrite(bool err) {
    _write << 1;
    if (err) {
        _write_error << 1;
    }
}

double MVCCValue::PessimismDegree() {
    auto c = (double)_write_error_window.get_value() /
             (double)_write_window.get_value();
    auto l = (double)_tx_op_after_write_num.get_value() /
             (double)_tx_op_num_window.get_value();
    return FLAGS_alpha * c + (1 - FLAGS_alpha) * l;
}

}  // namespace txindex
}  // namespace azino