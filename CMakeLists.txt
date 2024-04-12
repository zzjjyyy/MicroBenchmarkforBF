cmake_minimum_required(VERSION 2.8.12)

project(MicroBench)

if (CMAKE_VERSION VERSION_LESS "3.1")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
else ()
  set (CMAKE_CXX_STANDARD 17)
endif ()

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx -mavx2")
set(CMAKE_BUILD_TYPE "release")

find_package(Arrow REQUIRED)

include_directories(./include)

aux_source_directory(. SOURCES)

add_executable(
    ${PROJECT_NAME}
    ${SOURCES})

target_link_libraries(${PROJECT_NAME} Arrow::arrow_shared)