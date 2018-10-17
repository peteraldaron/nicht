cmake_minimum_required(VERSION 3.12)
project(zmqpubsub)
set(CMAKE_CXX_STANDARD 17)

include(ExternalProject)
# Find
find_package(PkgConfig)
find_program(MAKE_EXE NAMES gmake nmake make)
find_library(ZeroMQ_LIBRARY
        NAMES zmq
        PATHS ${PC_ZeroMQ_LIBRARY_DIRS})

pkg_check_modules(PC_ZeroMQ QUIET zmq)

# sub deps

set(EXTERNAL_INSTALL_LOCATION ${CMAKE_BINARY_DIR}/external)

ExternalProject_Add(
        fmt
        URL https://github.com/fmtlib/fmt/archive/5.2.1.zip
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${EXTERNAL_INSTALL_LOCATION} -DFMT_TEST=0 -DFMT_DOC=0
)

ExternalProject_Add(
        cppzmq
        URL https://github.com/zeromq/cppzmq/archive/master.zip
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${EXTERNAL_INSTALL_LOCATION} -DCPPZMQ_BUILD_TESTS=0
)

include_directories(${EXTERNAL_INSTALL_LOCATION}/include /usr/local/include)

add_executable(zmqpubsub main.cpp)
add_dependencies(zmqpubsub fmt cppzmq)

target_link_libraries(zmqpubsub ${ZeroMQ_LIBRARY} ${EXTERNAL_INSTALL_LOCATION}/lib/libfmt.a)
