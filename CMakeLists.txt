cmake_minimum_required(VERSION 3.13)
project(kv_app LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Seastar REQUIRED)

add_executable(kv-app
    main.cc
    webapi.cc
    store.cc
    store_loader.cc
    key.cc
    value.cc
    lru_cache.cc
    utf8.cc)
target_link_libraries(kv-app PRIVATE Seastar::seastar)

file(READ ${CMAKE_CURRENT_SOURCE_DIR}/openapi.yaml KV_OPENAPI_SPEC)
configure_file(openapi_spec.hh.in ${CMAKE_CURRENT_BINARY_DIR}/generated/openapi_spec.hh @ONLY)
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS openapi.yaml)
target_include_directories(kv-app PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/generated)
