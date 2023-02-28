#!/bin/bash

CURRENTDIR=$(pwd)

if [ -d "${CURRENTDIR}/third_party/incubator-brpc/output" ];then
    echo "Alread built brpc."
else
    cd "${CURRENTDIR}"/third_party/incubator-brpc && sh config_brpc.sh --headers=/usr/include --libs=/usr/lib && make -j"$(nproc)"
fi

rc=0
cd "${CURRENTDIR}" || exit
rm -rf tmp.sqPg7ubHww && rm -rf build && rm -rf cmake-build-debug- && \
mkdir build && cd build && cmake .. && make -j$(nproc)
rc=$?
if [ $rc -ne 0 ]; then
    echo "Build failed"
    exit $rc
else
    echo "Build succeeded"
fi

rc=0
cd "${CURRENTDIR}" || exit
rm -rf output && mkdir -p output/test
find build | grep -E '(txindex_server|txplanner_server|storage_server|azino_client|dummy_bench|ycsb)$' | xargs -i cp -v {} output
find build | grep -E '(test_[^\.]+)$' | xargs -i cp -v {} output/test
cp -v run_all_tests.sh output/test
cp -v setup.sh output
cp -v kill.sh output



