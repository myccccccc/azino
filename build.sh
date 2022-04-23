#!/bin/bash

CURRENTDIR=$(pwd)

if [ -d "${CURRENTDIR}/third_party/incubator-brpc/output" ];then
    echo "Alread built brpc."
else
    cd ${CURRENTDIR}/third_party/incubator-brpc && sh config_brpc.sh --headers=/usr/include --libs=/usr/lib && make -j"$(nproc)" && cd ${CURRENTDIR}
fi

rc=0
rm -rf tmp.sqPg7ubHww && rm -rf build && rm -rf cmake-build-debug- && \
mkdir build && cd build && cmake .. && make -j$(nproc) && cd .. && \
rm -rf output && mkdir -p output/test && \
find ./build | grep -E '(txindex_server|txplanner_server|storage_server|azino_client)$' | xargs -i cp {} output && \
find ./build | grep -E '(test_[^\.]+)$' | xargs -i cp {} output/test && \
cp run_all_tests.sh output/test
rc=$?
if [ $rc -ne 0 ]; then
    echo "Build failed"
else
    echo "Build succeeded"
fi
exit $rc



