#ifndef AZINO_INCLUDE_PARTITION_H
#define AZINO_INCLUDE_PARTITION_H

#include <map>
#include <string>
#include <utility>

#include "azino/range.h"
#include "service/partition.pb.h"

namespace azino {
typedef std::string TxIndex;  // txindex addresses in form of "0.0.0.0:8000"
typedef std::string Storage;  // storage addresses in form of "0.0.0.0:8000"

class PartitionConfig {
   public:
    PartitionConfig(TxIndex t) : txindex(std::move(t)) {}
    TxIndex GetTxIndex() const { return txindex; }

    PartitionConfigPB ToPB() const {
        PartitionConfigPB pb;
        pb.set_txindex(txindex);
        return pb;
    }

    static PartitionConfig FromPB(const PartitionConfigPB& pb) {
        return PartitionConfig(pb.txindex());
    }

   private:
    TxIndex txindex;
};

typedef std::map<Range, PartitionConfig, RangeComparator> PartitionConfigMap;

class Partition {
   public:
    Partition() = default;
    Partition(PartitionConfigMap pcm, Storage s)
        : partition_configmap(std::move(pcm)), storage(std::move(s)) {}
    inline const PartitionConfigMap& GetPartitionConfigMap() const {
        return partition_configmap;
    }
    inline void SetPartitionConfigMap(const PartitionConfigMap& pcm) {
        partition_configmap = pcm;
    }
    inline const Storage GetStorage() const { return storage; }

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
    Storage storage;
};
}  // namespace azino

#endif  // AZINO_INCLUDE_PARTITION_H
