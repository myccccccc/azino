project(azino_storage C CXX)

include_directories(${PROJECT_SOURCE_DIR}/include)

add_library(${PROJECT_NAME} STATIC ${PROJECT_SOURCE_DIR}/leveldb/dbimpl.cpp
                                   ${PROJECT_SOURCE_DIR}/service/storageserviceimpl.cpp
                                    ${PROJECT_SOURCE_DIR}/src/utils.cpp)
add_library(azino_storage::lib ALIAS ${PROJECT_NAME})

add_executable(storage_server ${PROJECT_SOURCE_DIR}/main.cpp
                              )

target_link_libraries(storage_server
                        azino_storage::lib
                        azino::lib
                        ${BRPC_LIB}
                        ${DYNAMIC_LIB})

# build tests
enable_testing()

add_executable(test_dbimpl  ${PROJECT_SOURCE_DIR}/test/test_dbimpl.cpp
                            )
target_link_libraries(test_dbimpl
                        azino_storage::lib
                        azino::lib
                        gtest_main
                        ${BRPC_LIB}
                        ${DYNAMIC_LIB}
                        ${PROTOBUF_LIBRARIES})

include(GoogleTest)
gtest_discover_tests(test_dbimpl)
