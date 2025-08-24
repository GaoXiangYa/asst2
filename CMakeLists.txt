cmake_minimum_required(VERSION 3.20)

project(cs149_asst2 C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_BUILD_TYPE Release)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libstdc++")
set(CMAKE_CXX_COMPILER clang++)
set(CMAKE_C_COMPILER clang)

option(BUILD_PART_A "build part_a" OFF)
message(STATUS "BUILD_PART_A: " ${BUILD_PART_A})

option(BUILD_PART_B "build_part_b" OFF)
message(STATUS "BUILD_PART_B: " ${BUILD_PART_B})

include_directories(${PROJECT_SOURCE_DIR}/common
    ${PROJECT_SOURCE_DIR}/tests/)

if(BUILD_PART_A)
    include_directories(${PROJECT_SOURCE_DIR}/part_a)
    add_executable(runtasks ${PROJECT_SOURCE_DIR}/part_a/tasksys.cpp ${PROJECT_SOURCE_DIR}/tests/main.cpp)
endif()


if (BUILD_PART_B)
    include(${PROJECT_SOURCE_DIR}/part_b/*.h)
    add_executable(runtasks PRIVATE ${PROJECT_SOURCE_DIR}/part_b/tasksys.cpp ${PROJECT_SOURCE_DIR}/tests/main.cpp)
endif()