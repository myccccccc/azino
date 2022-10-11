project(azino_txplanner C CXX)

include_directories(${PROJECT_SOURCE_DIR}/include)

add_library(${PROJECT_NAME} STATIC ${PROJECT_SOURCE_DIR}/service/txserviceimpl.cpp ${PROJECT_SOURCE_DIR}/service/dependenceserviceimpl.cpp
                                    )
add_library(azino_txplanner::lib ALIAS ${PROJECT_NAME})

add_executable(txplanner_server ${PROJECT_SOURCE_DIR}/main.cpp
        )

target_link_libraries(txplanner_server
        azino_txplanner::lib
        azino::lib
        ${BRPC_LIB}
        ${DYNAMIC_LIB})