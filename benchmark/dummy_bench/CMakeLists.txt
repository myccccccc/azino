project(azino_benchmark C CXX)

include_directories(${PROJECT_SOURCE_DIR}/include)

add_executable(dummy_bench ${PROJECT_SOURCE_DIR}/dummy_bench.cpp)

target_link_libraries(dummy_bench
        azino_sdk::lib
        azino::lib
        ${BRPC_LIB}
        ${COMMON_LIB})
