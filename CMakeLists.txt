cmake_minimum_required(VERSION 3.20)

project(snowy VERSION 0.1.0 LANGUAGES CXX)

option(SNOWY_BUILD_TESTS "Build Snowy tests" ON)
find_package(Threads REQUIRED)

add_library(snowy INTERFACE)
add_library(snowy::snowy ALIAS snowy)

target_compile_features(snowy INTERFACE cxx_std_20)
target_link_libraries(snowy INTERFACE Threads::Threads)
target_include_directories(
    snowy
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 11)
    target_compile_options(snowy INTERFACE -fcoroutines)
endif()

if(SNOWY_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
