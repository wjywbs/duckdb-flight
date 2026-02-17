cmake_minimum_required(VERSION 2.8.12...3.29)

project(FlightExtension)

set(TARGET_NAME flight)

include_directories(src/include)
include_directories(${CMAKE_SOURCE_DIR}/third_party/arrow/cpp/src)
include_directories(${CMAKE_BINARY_DIR}/third_party_arrow/src)

set(FLIGHT_EXTENSION_FILES
    src/duckdb_flight_sql_server.cpp
    src/flight_extension.cpp
    src/flight_service.cpp)

# Build Arrow Flight SQL from the vendored Arrow source tree when this extension is enabled.
if(NOT TARGET arrow_flight_sql_static)
  set(FLIGHT_PREV_CXX_STANDARD ${CMAKE_CXX_STANDARD})
  set(FLIGHT_PREV_POSITION_INDEPENDENT_CODE ${CMAKE_POSITION_INDEPENDENT_CODE})
  set(CMAKE_CXX_STANDARD 20)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  set(CMAKE_POSITION_INDEPENDENT_CODE ON)

  set(ARROW_BUILD_STATIC ON CACHE BOOL "" FORCE)
  set(ARROW_BUILD_SHARED OFF CACHE BOOL "" FORCE)
  set(ARROW_DEPENDENCY_SOURCE BUNDLED CACHE STRING "" FORCE)
  set(ARROW_ENABLE_THREADING ON CACHE BOOL "" FORCE)
  set(ARROW_SIMD_LEVEL NONE CACHE STRING "" FORCE)
  set(ARROW_RUNTIME_SIMD_LEVEL NONE CACHE STRING "" FORCE)
  set(ARROW_BUILD_TESTS OFF CACHE BOOL "" FORCE)
  set(ARROW_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
  set(ARROW_BUILD_UTILITIES OFF CACHE BOOL "" FORCE)
  set(ARROW_BUILD_INTEGRATION OFF CACHE BOOL "" FORCE)
  set(ARROW_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
  set(ARROW_COMPUTE OFF CACHE BOOL "" FORCE)
  set(ARROW_IPC ON CACHE BOOL "" FORCE)
  set(ARROW_DATASET OFF CACHE BOOL "" FORCE)
  set(ARROW_FILESYSTEM OFF CACHE BOOL "" FORCE)
  set(ARROW_PARQUET OFF CACHE BOOL "" FORCE)
  set(ARROW_JSON OFF CACHE BOOL "" FORCE)
  set(ARROW_WITH_ZLIB OFF CACHE BOOL "" FORCE)
  set(ARROW_WITH_ZSTD OFF CACHE BOOL "" FORCE)
  set(ARROW_WITH_BROTLI OFF CACHE BOOL "" FORCE)
  set(ARROW_WITH_BZ2 OFF CACHE BOOL "" FORCE)
  set(ARROW_WITH_LZ4 OFF CACHE BOOL "" FORCE)
  set(ARROW_WITH_SNAPPY OFF CACHE BOOL "" FORCE)
  set(ARROW_FLIGHT ON CACHE BOOL "" FORCE)
  set(ARROW_FLIGHT_SQL ON CACHE BOOL "" FORCE)
  set(ARROW_POSITION_INDEPENDENT_CODE ON CACHE BOOL "" FORCE)
  get_directory_property(FLIGHT_SAVED_INCLUDE_DIRECTORIES INCLUDE_DIRECTORIES)
  set_directory_properties(PROPERTIES INCLUDE_DIRECTORIES "")
  add_subdirectory(${CMAKE_SOURCE_DIR}/third_party/arrow/cpp ${CMAKE_BINARY_DIR}/third_party_arrow EXCLUDE_FROM_ALL)
  set_directory_properties(PROPERTIES INCLUDE_DIRECTORIES "${FLIGHT_SAVED_INCLUDE_DIRECTORIES}")

  set(CMAKE_POSITION_INDEPENDENT_CODE ${FLIGHT_PREV_POSITION_INDEPENDENT_CODE})
  set(CMAKE_CXX_STANDARD ${FLIGHT_PREV_CXX_STANDARD})
endif()

set(PARAMETERS "-warnings")
set(FLIGHT_PREV_EXTENSION_STATIC_BUILD ${EXTENSION_STATIC_BUILD})
set(EXTENSION_STATIC_BUILD ON)
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${FLIGHT_EXTENSION_FILES})
set(EXTENSION_STATIC_BUILD ${FLIGHT_PREV_EXTENSION_STATIC_BUILD})
set_target_properties(${TARGET_NAME}_loadable_extension PROPERTIES CXX_STANDARD 20 CXX_STANDARD_REQUIRED ON)

target_link_libraries(${TARGET_NAME}_loadable_extension
                      arrow_flight_sql_static
                      arrow_flight_static
                      arrow_static)

add_executable(flight_sql_smoke_client test/client/flight_sql_smoke_client.cpp)
set_target_properties(flight_sql_smoke_client PROPERTIES
                      CXX_STANDARD 20
                      CXX_STANDARD_REQUIRED ON
                      RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/extension/flight")

target_include_directories(flight_sql_smoke_client PRIVATE
                           src/include
                           ${CMAKE_SOURCE_DIR}/third_party/arrow/cpp/src
                           ${CMAKE_BINARY_DIR}/third_party_arrow/src)

target_link_libraries(flight_sql_smoke_client
                      arrow_flight_sql_static
                      arrow_flight_static
                      arrow_static)
