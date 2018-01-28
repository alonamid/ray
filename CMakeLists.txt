cmake_minimum_required(VERSION 3.4)

project(ray)

set(CMAKE_INCLUDE_SYSTEM_FLAG_CXX "-I ") #for gcc 6 and higher

#set(ARROW_DIR "${CMAKE_CURRENT_LIST_DIR}/src/thirdparty/arrow/"
set(ARROW_DIR ${CMAKE_CURRENT_LIST_DIR}/../../../python-pyarrow/0.7.1-r0/recipe-sysroot/usr/
    CACHE STRING "Path of the arrow source directory")

#include_directories("${ARROW_DIR}/cpp/src/")
include_directories("${ARROW_DIR}/include/arrow/")

add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/src/common/)
add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/src/plasma/)
add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/src/local_scheduler/)
add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/src/global_scheduler/)
