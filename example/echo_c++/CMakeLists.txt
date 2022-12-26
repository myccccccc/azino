
cmake_minimum_required(VERSION 3.5)
project(echo_c++ C CXX)

protobuf_generate_cpp(PROTO_SRC PROTO_HEADER echo.proto)
# include PROTO_HEADER
include_directories(${CMAKE_CURRENT_BINARY_DIR})

add_executable(echo_client client.cpp ${PROTO_SRC} ${PROTO_HEADER})
add_executable(echo_server server.cpp ${PROTO_SRC} ${PROTO_HEADER})

target_link_libraries(echo_client ${BRPC_LIB} ${COMMON_LIB})
target_link_libraries(echo_server ${BRPC_LIB} ${COMMON_LIB})
