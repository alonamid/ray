cmake_minimum_required(VERSION 3.4)

project(ray)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wl,--no-as-needed -ldl -pg -g3 ") #gprof profiling
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--no-as-needed -ldl -pg -g3 ") #gprof profiling

set(ARROW_DIR "${CMAKE_CURRENT_LIST_DIR}/src/thirdparty/arrow/"
    CACHE STRING "Path of the arrow source directory")

include_directories("${ARROW_DIR}/cpp/src/")

add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/src/common/)
add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/src/plasma/)
add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/src/local_scheduler/)
add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/src/global_scheduler/)
