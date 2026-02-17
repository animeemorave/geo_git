cmake_minimum_required(VERSION 3.15)
project(GeoVersionControl VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)


find_package(mongocxx REQUIRED PATHS /usr/local/lib/cmake)
find_package(bsoncxx REQUIRED PATHS /usr/local/lib/cmake)
find_package(OpenSSL REQUIRED)

include_directories(${CMAKE_SOURCE_DIR}/src)

set(SOURCES
    src/main.cpp
    src/storage/mongodb_connection/mongodb_connection.cpp
    src/storage/bpo_storage/bpo_storage.cpp
    src/storage/cas/cas.cpp
    src/utils/logger/logger.cpp
)

add_executable(geoversion ${SOURCES})

target_link_libraries(geoversion
    PRIVATE
    mongo::mongocxx_shared
    mongo::bsoncxx_shared
    OpenSSL::SSL
    OpenSSL::Crypto
)

target_compile_options(geoversion PRIVATE
    -Wall
    -Wextra
    -Wpedantic
)
