cmake_minimum_required (VERSION 3.10)
project (holistic-aggregates)

include_directories("${PROJECT_SOURCE_DIR}")
include_directories(${PROJECT_SOURCE_DIR}/thirdparty)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --std=c++17")
if ("${CMAKE_BUILD_TYPE}" STREQUAL Debug)
   set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
ENDIF()

# The binaries
add_executable(bench bench.cpp)
add_executable(scalability_bench scalability_bench.cpp)

# The tests

set(TEST_SRC
  test/aggregatedistinct.cpp
  test/losertree.cpp
  test/percentile.cpp
  test/rank.cpp
  test/mergesorttree.cpp
)

add_executable(run_tests test/main.cpp ${TEST_SRC})
