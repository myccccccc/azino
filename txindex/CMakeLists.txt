project(azino_txindex C CXX)

include_directories(${PROJECT_SOURCE_DIR}/include)

add_library(${PROJECT_NAME} STATIC ${PROJECT_SOURCE_DIR}/src/kvregion.cpp
                                   ${PROJECT_SOURCE_DIR}/src/txopserviceimpl.cpp
                                   ${PROJECT_SOURCE_DIR}/src/persist.cpp
                                   ${PROJECT_SOURCE_DIR}/src/depedence.cpp
                                   ${PROJECT_SOURCE_DIR}/src/kvbucket.cpp
                                   ${PROJECT_SOURCE_DIR}/src/mvccvalue.cpp
                                   ${PROJECT_SOURCE_DIR}/src/txindex.cpp
                                   ${PROJECT_SOURCE_DIR}/src/metric.cpp
                                   ${PROJECT_SOURCE_DIR}/src/partition_manager.cpp)

add_library(azino_txindex::lib ALIAS ${PROJECT_NAME})

add_executable(txindex_server ${PROJECT_SOURCE_DIR}/main.cpp)

target_link_libraries(txindex_server
                        azino_txindex::lib
                        azino::lib
                        ${BRPC_LIB}
                        ${COMMON_LIB}
                        )

# build tests
enable_testing()

add_executable(test_txindeximpl  ${PROJECT_SOURCE_DIR}/test/test_txindeximpl.cpp)
target_link_libraries(test_txindeximpl
                        azino_txindex::lib
                        azino::lib
                        gtest_main
                        ${BRPC_LIB}
                        ${COMMON_LIB})

include(GoogleTest)
gtest_discover_tests(test_txindeximpl)