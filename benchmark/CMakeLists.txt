project(azino_benchmark C CXX)

include_directories(${PROJECT_SOURCE_DIR}/include)

add_executable(dummy_benchmark ${PROJECT_SOURCE_DIR}/dummy_benchmark.cpp)

target_link_libraries(dummy_benchmark
        azino_sdk::lib
        azino::lib
        ${BRPC_LIB}
        ${DYNAMIC_LIB})
