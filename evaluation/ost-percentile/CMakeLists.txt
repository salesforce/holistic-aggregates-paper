# To build:
# cmake .. -DCMAKE_PREFIX_PATH=/usr/local/lib/cmake/TBB -GNinja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_EXPORT_COMPILE_COMMANDS=1

cmake_minimum_required (VERSION 3.10)
project (ost-percentile)

include_directories("${PROJECT_SOURCE_DIR}")

find_package(Threads REQUIRED)
find_package(TBB REQUIRED COMPONENTS tbb)


set(CMAKE_CXX_STANDARD 20)
if ("${CMAKE_BUILD_TYPE}" STREQUAL Debug)
   set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
ENDIF()

# The binaries
add_executable(ost_percentile main.cpp tree234.c)
target_link_libraries(ost_percentile TBB::tbb Threads::Threads)
