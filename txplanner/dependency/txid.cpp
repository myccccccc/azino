#include "txid.h"

std::hash<uint64_t> hash;

namespace azino {
namespace txplanner {
bool TxIDPtrEqual::operator()(const TxIDPtr& c1, const TxIDPtr& c2) const {
    return c1->id() == c2->id();
}

std::size_t TxIDPtrHash::operator()(const TxIDPtr& c) const {
    return hash(c->id());
}
void TxID::early_validate() {
    if (early_validation_response && early_validation_done) {
        early_validation_response->set_allocated_txid(new TxIdentifier(txid));
        early_validation_done->Run();
    }
}

void TxID::del_dep(TxIDPtr self) {
    for (const auto& dp : in) {
        dp->out.erase(self);
    }
    for (const auto& dp : out) {
        dp->in.erase(self);
    }
    in.clear();
    out.clear();
}
}  // namespace txplanner
}  // namespace azino