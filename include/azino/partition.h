#ifndef AZINO_INCLUDE_PARTITION_H
#define AZINO_INCLUDE_PARTITION_H

#include <map>
#include <string>
#include <unordered_set>
#include <utility>

#include "azino/range.h"
#include "service/partition.pb.h"

namespace azino {
class PartitionConfig {
   public:
    PartitionConfig(std::string t) : txindex(std::move(t)) {}
    std::string GetTxIndex() const { return txindex; }
    const std::unordered_set<std::string>& GetPessimismKey() const {
        return pk;
    }

    PartitionConfigPB ToPB() const {
        PartitionConfigPB pb;
        pb.set_txindex(txindex);
        for (auto& key : pk) {
            pb.add_pessimism_key(key);
        }
        return pb;
    }

    static PartitionConfig FromPB(const PartitionConfigPB& pb) {
        auto res = PartitionConfig(pb.txindex());
        for (int i = 0; i < pb.pessimism_key_size(); i++) {
            res.pk.insert(pb.pessimism_key(i));
        }
        return res;
    }

   private:
    std::string txindex;  // txindex addresses in form of "0.0.0.0:8000"
    std::unordered_set<std::string> pk;
};

typedef std::map<Range, PartitionConfig, RangeComparator> PartitionConfigMap;

class Partition {
   public:
    Partition() = default;
    Partition(PartitionConfigMap pcm, std::string s)
        : partition_configmap(std::move(pcm)), storage(std::move(s)) {}
    inline const PartitionConfigMap& GetPartitionConfigMap() const {
        return partition_configmap;
    }
    inline void SetPartitionConfigMap(const PartitionConfigMap& pcm) {
        partition_configmap = pcm;
    }
    inline const std::string GetStorage() const { return storage; }

    PartitionPB ToPB() const {
        PartitionPB pb;
        PartitionConfigMapPB* pcm = new PartitionConfigMapPB();
        pb.set_storage(storage);
        pb.set_allocated_pcm(pcm);

        for (auto iter = partition_configmap.begin();
             iter != partition_configmap.end(); iter++) {
            *(pcm->add_ranges()) = iter->first.ToPB();
            *(pcm->add_partition_configs()) = iter->second.ToPB();
        }

        return pb;
    }

    static Partition FromPB(const PartitionPB& pb) {
        PartitionConfigMap pcm;
        for (int i = 0; i < pb.pcm().ranges_size(); i++) {
            pcm.insert(std::make_pair(
                Range::FromPB(pb.pcm().ranges(i)),
                PartitionConfig::FromPB(pb.pcm().partition_configs(i))));
        }
        return Partition(pcm, pb.storage());
    }

   private:
    PartitionConfigMap partition_configmap;
    std::string storage;  // storage addresses in form of "0.0.0.0:8000"
};
}  // namespace azino

#endif  // AZINO_INCLUDE_PARTITION_H
