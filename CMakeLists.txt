cmake_minimum_required(VERSION 3.12)
project(dp2)

set(CMAKE_CXX_STANDARD 17)
add_subdirectory(rapidjson)
include_directories(env/include/python3.7m env/include/site/python3.7 rapidjson/include)

add_executable(dp2 pos_parser.cc pos_parser.h parse.cc)