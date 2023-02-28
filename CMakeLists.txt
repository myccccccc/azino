cmake_minimum_required(VERSION 3.5)
project(azino C CXX)

# project include
include_directories(${PROJECT_SOURCE_DIR}/include)

# protobuf
set (Protobuf_USE_STATIC_LIBS ON)
include(FindProtobuf)
if (Protobuf_FOUND)
    message( STATUS "Protobuf version : ${Protobuf_VERSION}" )
    message( STATUS "Protobuf include path : ${Protobuf_INCLUDE_DIRS}" )
    message( STATUS "Protobuf libraries : ${Protobuf_LIBRARIES}" )
endif()

# zlib
find_path(ZLIB_INCLUDE_PATH NAMES zlib.h)
find_library(ZLIB_LIB NAMES libz.a)
if ((NOT ZLIB_INCLUDE_PATH) OR (NOT ZLIB_LIB))
    message(FATAL_ERROR "Fail to find zlib")
endif()
message( STATUS "zlib libraries : ${ZLIB_LIB}" )
include_directories(${ZLIB_INCLUDE_PATH})

# brpc
set(BRPC_PATH "${PROJECT_SOURCE_DIR}/third_party/incubator-brpc/output")
message(STATUS "Set brpc path ${BRPC_PATH}")
set(CMAKE_PREFIX_PATH ${BRPC_PATH})
find_path(BRPC_INCLUDE_PATH NAMES brpc/server.h)
find_library(BRPC_LIB NAMES libbrpc.a)
if((NOT BRPC_INCLUDE_PATH) OR (NOT BRPC_LIB))
    message(FATAL_ERROR "Fail to find brpc")
endif()
message( STATUS "brpc libraries : ${BRPC_LIB}" )
include_directories(${BRPC_INCLUDE_PATH})

# gflag
find_path(GFLAGS_INCLUDE_PATH gflags/gflags.h)
find_library(GFLAGS_LIBRARY NAMES libgflags.a)
if((NOT GFLAGS_INCLUDE_PATH) OR (NOT GFLAGS_LIBRARY))
    message(FATAL_ERROR "Fail to find gflags")
endif()
message( STATUS "gflag libraries : ${GFLAGS_LIBRARY}" )
include_directories(${GFLAGS_INCLUDE_PATH})
execute_process(
        COMMAND bash -c "grep \"namespace [_A-Za-z0-9]\\+ {\" ${GFLAGS_INCLUDE_PATH}/gflags/gflags_declare.h | head -1 | awk '{print $2}' | tr -d '\n'"
        OUTPUT_VARIABLE GFLAGS_NS
)
if(${GFLAGS_NS} STREQUAL "GFLAGS_NAMESPACE")
    execute_process(
            COMMAND bash -c "grep \"#define GFLAGS_NAMESPACE [_A-Za-z0-9]\\+\" ${GFLAGS_INCLUDE_PATH}/gflags/gflags_declare.h | head -1 | awk '{print $3}' | tr -d '\n'"
            OUTPUT_VARIABLE GFLAGS_NS
    )
endif()
message(STATUS "gflags namespace ${GFLAGS_NS}")

# snappy
find_path(SNAPPY_INCLUDE_PATH NAMES snappy.h)
find_library(SNAPPY_LIB NAMES libsnappy.a)
if ((NOT SNAPPY_INCLUDE_PATH) OR (NOT SNAPPY_LIB))
    message(FATAL_ERROR "Fail to find snappy")
endif()
message( STATUS "snappy libraries : ${SNAPPY_LIB}" )
include_directories(${SNAPPY_INCLUDE_PATH})

# leveldb
find_path(LEVELDB_INCLUDE_PATH NAMES leveldb/db.h)
find_library(LEVELDB_LIB NAMES libleveldb.a)
if ((NOT LEVELDB_INCLUDE_PATH) OR (NOT LEVELDB_LIB))
    message(FATAL_ERROR "Fail to find leveldb")
endif()
message( STATUS "leveldb libraries : ${LEVELDB_LIB}" )
include_directories(${LEVELDB_INCLUDE_PATH})

# openssl
set(OPENSSL_USE_STATIC_LIBS TRUE)
find_package(OpenSSL)
include_directories(${OPENSSL_INCLUDE_DIR})
message( STATUS "openssl crypto libraries : ${OPENSSL_CRYPTO_LIBRARY}" )
message( STATUS "openssl ssl libraries : ${OPENSSL_SSL_LIBRARY}" )

# compiler options
set(CMAKE_CPP_FLAGS "${DEFINE_CLOCK_GETTIME} -DGFLAGS_NS=${GFLAGS_NS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CPP_FLAGS} -g -O0 -D__const__= -pipe -W -Wall -Wno-unused-parameter -fPIC -fno-omit-frame-pointer -faligned-new -static-libgcc -static-libstdc++")
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(COMMON_LIB
        pthread
        ${GFLAGS_LIBRARY}
        ${PROTOBUF_LIBRARIES}
        ${ZLIB_LIB}
        ${LEVELDB_LIB}
        ${SNAPPY_LIB}
        ${OPENSSL_SSL_LIBRARY}
        ${OPENSSL_CRYPTO_LIBRARY}
        dl
        )


file(GLOB protobuf_files
        service/*.proto
        service/storage/*.proto
        service/txplanner/*.proto
        service/txindex/*.proto)
SET(PROTO_META_BASE_DIR ${CMAKE_CURRENT_BINARY_DIR})
LIST(APPEND PROTO_FLAGS -I${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_SOURCE_DIR})
FOREACH(FIL ${protobuf_files})
    GET_FILENAME_COMPONENT(FIL_WE ${FIL} NAME_WE)
    string(REGEX REPLACE ".+/(.+)\\..*" "\\1" FILE_NAME ${FIL})
    string(REGEX REPLACE "(.+)\\${FILE_NAME}.*" "\\1" FILE_PATH ${FIL})
    string(REGEX MATCH "(/service/txindex.*|/service/txplanner.*|/service/storage.*|/service.*)" OUT_PATH ${FILE_PATH})

    set(PROTO_SRCS "${CMAKE_CURRENT_BINARY_DIR}${OUT_PATH}${FIL_WE}.pb.cc")
    set(PROTO_HDRS "${CMAKE_CURRENT_BINARY_DIR}${OUT_PATH}${FIL_WE}.pb.h")

    EXECUTE_PROCESS(
            COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} ${PROTO_FLAGS} --cpp_out=${PROTO_META_BASE_DIR} ${FIL}
    )

    message("Copying " ${PROTO_SRCS} " " ${PROTO_HDRS}" to " ${FILE_PATH})
    file(COPY ${PROTO_SRCS} DESTINATION ${FILE_PATH})
    file(COPY ${PROTO_HDRS} DESTINATION ${FILE_PATH})
ENDFOREACH()


file(GLOB protobuf_srcs
        service/*.pb.cc
        service/storage/*.pb.cc
        service/txplanner/*.pb.cc
        service/txindex/*.pb.cc)
add_library(${PROJECT_NAME} STATIC ${protobuf_srcs})
add_library(azino::lib ALIAS ${PROJECT_NAME})

# build gtest
add_subdirectory(third_party/googletest)

# build every component
add_subdirectory(storage)
add_subdirectory(txindex)
add_subdirectory(txplanner)
add_subdirectory(sdk)

# build examples
add_subdirectory(example/echo_c++)
add_subdirectory(example/client)

# build benchmark
add_subdirectory(benchmark/dummy_bench)
add_subdirectory(benchmark/YCSB-cpp)