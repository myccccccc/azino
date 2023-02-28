project(azino_client C CXX)

include_directories(${PROJECT_SOURCE_DIR}/include)

add_executable(azino_client ${PROJECT_SOURCE_DIR}/main.cpp
                            )

target_link_libraries(azino_client
                        azino_sdk::lib
                        azino::lib
                        ${BRPC_LIB}
                        ${COMMON_LIB})
