cmake_minimum_required(VERSION 3.20)

project(snowy VERSION 0.1.0 LANGUAGES CXX)

option(SNOWY_BUILD_TESTS "Build Snowy tests" ON)
option(SNOWY_BUILD_EXAMPLES "Build Snowy examples" ON)
find_package(Threads REQUIRED)

add_library(snowy STATIC src/loop.cpp src/socket.cpp)
add_library(snowy::snowy ALIAS snowy)

target_compile_features(snowy PUBLIC cxx_std_20)
target_link_libraries(snowy PUBLIC Threads::Threads)
target_include_directories(
    snowy
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 11)
    target_compile_options(snowy PUBLIC -fcoroutines)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    find_path(URING_INCLUDE_DIR liburing.h REQUIRED)
    find_library(URING_LIBRARY uring REQUIRED)
    target_sources(snowy PRIVATE src/uring.cpp)
    target_include_directories(snowy SYSTEM PRIVATE ${URING_INCLUDE_DIR})
    target_link_libraries(snowy PRIVATE ${URING_LIBRARY})
elseif(APPLE)
    target_sources(snowy PRIVATE src/kqueue.cpp)
elseif(WIN32)
    target_sources(snowy PRIVATE src/iocp.cpp)
    target_link_libraries(snowy PUBLIC ws2_32)
    target_compile_definitions(snowy PUBLIC _WIN32_WINNT=0x0602 WIN32_LEAN_AND_MEAN NOMINMAX)
else()
    message(FATAL_ERROR "Snowy requires Linux, macOS, or Windows")
endif()
if(MSVC)
    target_compile_options(snowy PRIVATE /W4 /WX /permissive-)
else()
    target_compile_options(snowy PRIVATE -Wall -Wextra -Wpedantic -Werror)
endif()

if(SNOWY_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

if(SNOWY_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()
