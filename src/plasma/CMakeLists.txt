cmake_minimum_required(VERSION 3.4)

project(plasma)

# Recursively include common
include(${CMAKE_CURRENT_LIST_DIR}/../common/cmake/Common.cmake)

# Include plasma
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/../thirdparty/arrow/python/cmake_modules)

#find_package(Plasma)
set(PLASMA_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/../../../../../python-pyarrow/0.7.1-r0/recipe-sysroot/usr/include)
include_directories(SYSTEM ${PLASMA_INCLUDE_DIR})

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --std=c99 -D_XOPEN_SOURCE=500 -D_POSIX_C_SOURCE=200809L -O3")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --std=c++11 -D_XOPEN_SOURCE=500 -D_POSIX_C_SOURCE=200809L -O3")

if(UNIX AND NOT APPLE)
  link_libraries(rt)
endif()

include_directories("${ARROW_DIR}/include/")
include_directories("${ARROW_DIR}/include/arrow")
include_directories("${ARROW_DIR}/include/plasma")
# include_directories("${CMAKE_CURRENT_LIST_DIR}/../")

include_directories("${FLATBUFFERS_INCLUDE_DIR}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

add_executable(plasma_manager
  plasma_manager.cc)

set(PLASMA_STATIC_LIB ${CMAKE_CURRENT_LIST_DIR}/../../../../../python-pyarrow/0.7.1-r0/recipe-sysroot/usr/lib/libplasma.a)

target_link_libraries(plasma_manager common ${PLASMA_STATIC_LIB} ${ARROW_DIR}/lib/libarrow.a -lpthread)

set(PLASMA_STATIC_LIB ${CMAKE_CURRENT_LIST_DIR}/../../../../../python-pyarrow/0.7.1-r0/recipe-sysroot/usr/lib/libplasma.a)

define_test(client_tests ${PLASMA_STATIC_LIB})
define_test(manager_tests ${PLASMA_STATIC_LIB} plasma_manager.cc)

install(TARGETS plasma_manager DESTINATION ${CMAKE_INSTALL_BINDIR}/ray/core/src/plasma)
#install(TARGETS plasma_manager DESTINATION ${CMAKE_BINARY_DIR}/ray/core/src/plasma)
add_custom_target(copy_plasma_manager ALL)
add_custom_command(TARGET copy_plasma_manager POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E
                      copy $<TARGET_FILE:plasma_manager>
                           ${CMAKE_CURRENT_LIST_DIR}/../../python/ray/core/src/plasma/plasma_manager)
add_custom_target(copy_plasma_store ALL)
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/../../../../../apache-arrow/0.7.1-r0/image/usr/bin/plasma-dir/plasma_store DESTINATION ${CMAKE_BINARY_DIR}/src/plasma/)
