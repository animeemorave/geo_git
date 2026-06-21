cmake_minimum_required(VERSION 3.15)
project(GeoGitGrpcServer VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(mongocxx REQUIRED PATHS /usr/local/lib/cmake)
find_package(bsoncxx REQUIRED PATHS /usr/local/lib/cmake)
find_package(OpenSSL REQUIRED)
find_package(Boost REQUIRED)
find_package(Protobuf REQUIRED)
find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(GRPCPP REQUIRED grpc++)

find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

set(SRC_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../src)
set(PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
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

set(ENGINE_SOURCES
    ${SRC_ROOT}/storage/mongodb_connection/mongodb_connection.cpp
    ${SRC_ROOT}/storage/bpo_storage/bpo_storage.cpp
    ${SRC_ROOT}/storage/cas/cas.cpp
    ${SRC_ROOT}/storage/situation_storage/situation_storage.cpp
    ${SRC_ROOT}/storage/branch_storage/branch_storage.cpp
    ${SRC_ROOT}/storage/version_storage/version_storage.cpp
    ${SRC_ROOT}/storage/delta_storage/delta_storage.cpp
    ${SRC_ROOT}/diff/diff_engine.cpp
    ${SRC_ROOT}/diff/status_engine.cpp
    ${SRC_ROOT}/diff/geometry_utils.cpp
    ${SRC_ROOT}/diff/entity_matcher.cpp
    ${SRC_ROOT}/diff/matchers/small_diff_matcher.cpp
    ${SRC_ROOT}/diff/matchers/large_diff_matcher.cpp
    ${SRC_ROOT}/diff/matchers/adaptive_matcher.cpp
    ${SRC_ROOT}/merge/merge_engine.cpp
    ${SRC_ROOT}/utils/logger/logger.cpp
    ${SRC_ROOT}/utils/uuid/uuid.cpp
)

set(SERVICE_SOURCES
    src/server/service_impl.cpp
    src/handlers/situation_handler.cpp
    src/handlers/branch_handler.cpp
    src/handlers/version_handler.cpp
    src/handlers/bpo_handler.cpp
    src/handlers/diff_handler.cpp
    src/handlers/merge_handler.cpp
    src/handlers/import_handler.cpp
)

add_library(geogit_service STATIC
    ${SERVICE_SOURCES}
    ${ENGINE_SOURCES}
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)

target_include_directories(geogit_service PUBLIC
    ${SRC_ROOT}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${GEN_DIR}
    ${Boost_INCLUDE_DIRS}
    ${GRPCPP_INCLUDE_DIRS}
)

target_link_directories(geogit_service PUBLIC ${GRPCPP_LIBRARY_DIRS})

target_link_libraries(geogit_service PUBLIC
    mongo::mongocxx_shared
    mongo::bsoncxx_shared
    OpenSSL::SSL
    OpenSSL::Crypto
    protobuf::libprotobuf
    ${GRPCPP_LIBRARIES}
    Threads::Threads
)

add_executable(geogit_server
    src/main.cpp
    src/server/grpc_server.cpp
)
target_link_libraries(geogit_server PRIVATE geogit_service)

enable_testing()
add_executable(test_grpc_integration tests/test_grpc_integration.cpp)
target_link_libraries(test_grpc_integration PRIVATE geogit_service)
add_test(NAME test_grpc_integration COMMAND test_grpc_integration)
