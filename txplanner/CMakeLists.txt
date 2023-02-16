project(azino_txplanner C CXX)

include_directories(${PROJECT_SOURCE_DIR}/include)

add_library(${PROJECT_NAME} STATIC ${PROJECT_SOURCE_DIR}/src/txserviceimpl.cpp
                                   ${PROJECT_SOURCE_DIR}/src/regionserviceimpl.cpp
                                   ${PROJECT_SOURCE_DIR}/src/txidtable.cpp
                                   ${PROJECT_SOURCE_DIR}/src/txid.cpp
                                   ${PROJECT_SOURCE_DIR}/src/gc.cpp
                                   ${PROJECT_SOURCE_DIR}/src/metric.cpp
                                   ${PROJECT_SOURCE_DIR}/src/partition_manager.cpp)

add_library(azino_txplanner::lib ALIAS ${PROJECT_NAME})

add_executable(txplanner_server ${PROJECT_SOURCE_DIR}/main.cpp
        )

target_link_libraries(txplanner_server
        azino_txplanner::lib
        azino::lib
        ${BRPC_LIB}
        ${DYNAMIC_LIB})

# build tests
enable_testing()

add_executable(test_txplanner  ${PROJECT_SOURCE_DIR}/test/test_txplanner.cpp)
target_link_libraries(test_txplanner
        azino_txplanner::lib
        azino::lib
        gtest_main
        ${BRPC_LIB}
        ${DYNAMIC_LIB})

include(GoogleTest)
gtest_discover_tests(test_txplanner)