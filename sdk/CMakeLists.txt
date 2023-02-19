project(azino_sdk C CXX)

include_directories(${PROJECT_SOURCE_DIR}/include)

add_library(${PROJECT_NAME} STATIC ${PROJECT_SOURCE_DIR}/src/client.cpp
                                   )
add_library(azino_sdk::lib ALIAS ${PROJECT_NAME})

target_link_libraries(azino_sdk)

# build tests
enable_testing()

add_executable(test_sdk  ${PROJECT_SOURCE_DIR}/test/test_sdk.cpp)
target_link_libraries(test_sdk
        azino_sdk::lib
        azino::lib
        gtest_main
        ${BRPC_LIB}
        ${DYNAMIC_LIB}
        ${PROTOBUF_LIBRARIES})

include(GoogleTest)
gtest_discover_tests(test_sdk)