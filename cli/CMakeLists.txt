cmake_minimum_required(VERSION 3.15)
project(GeoGitCli VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Protobuf REQUIRED)
find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(GRPCPP REQUIRED grpc++)

find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

set(PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../grpc/proto)
set(GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GEN_DIR})

set(PROTO_SRCS ${GEN_DIR}/geo_git.pb.cc)
set(PROTO_HDRS ${GEN_DIR}/geo_git.pb.h)
set(GRPC_SRCS ${GEN_DIR}/geo_git.grpc.pb.cc)
set(GRPC_HDRS ${GEN_DIR}/geo_git.grpc.pb.h)

add_custom_command(
    OUTPUT ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
    ARGS --proto_path=${PROTO_DIR}
         --proto_path=${Protobuf_INCLUDE_DIRS}
         --cpp_out=${GEN_DIR}
         --grpc_out=${GEN_DIR}
         --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
         ${PROTO_DIR}/geo_git.proto
    DEPENDS ${PROTO_DIR}/geo_git.proto
    COMMENT "Generating protobuf and gRPC sources"
)

add_executable(geogit
    src/main.cpp
    src/client.cpp
    src/commands.cpp
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)

target_include_directories(geogit PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${GEN_DIR}
    ${GRPCPP_INCLUDE_DIRS}
)

target_link_directories(geogit PRIVATE ${GRPCPP_LIBRARY_DIRS})

target_link_libraries(geogit PRIVATE
    protobuf::libprotobuf
    ${GRPCPP_LIBRARIES}
    Threads::Threads
)
