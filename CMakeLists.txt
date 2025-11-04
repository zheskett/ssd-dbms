cmake_minimum_required(VERSION 3.16)
project(ssd-dbms C)
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

add_compile_options(-Wall)

# Build the static library
file(GLOB_RECURSE SRC CONFIGURE_DEPENDS "${CMAKE_SOURCE_DIR}/src/*.c")
list(FILTER SRC EXCLUDE REGEX "(.*/src/main\\.c$|.*/src/cli_commands\\.c$)") # Exclude main.c and cli_commands.c
add_library(ssd-dbms STATIC ${SRC})
target_include_directories(ssd-dbms PUBLIC ${CMAKE_SOURCE_DIR}/include PRIVATE ${CMAKE_SOURCE_DIR}/src)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
target_link_libraries(ssd-dbms PUBLIC Threads::Threads)

# Build the CLI executable
add_executable(ssd-dbms-cli ${CMAKE_SOURCE_DIR}/src/main.c ${CMAKE_SOURCE_DIR}/src/cli_commands.c)
target_link_libraries(ssd-dbms-cli PRIVATE ssd-dbms)
target_include_directories(ssd-dbms-cli PRIVATE ${CMAKE_SOURCE_DIR}/include)

add_library(linenoise STATIC ${CMAKE_SOURCE_DIR}/vendor/linenoise/linenoise.c)
target_include_directories(linenoise PUBLIC ${CMAKE_SOURCE_DIR}/vendor/linenoise)
target_link_libraries(ssd-dbms-cli PRIVATE linenoise)

# Tests
option(BUILD_TESTS "Build unit tests" ON)
if(BUILD_TESTS)
  include(CTest)

  file(GLOB TEST_FILES CONFIGURE_DEPENDS ${CMAKE_SOURCE_DIR}/tests/test_*.c)

  # Adds an executable and a test for each test file
  foreach(tf ${TEST_FILES})
    get_filename_component(tname ${tf} NAME_WE)
    add_executable(${tname} ${tf} ${CMAKE_SOURCE_DIR}/vendor/unity/unity.c)
    target_include_directories(${tname} PRIVATE
      ${CMAKE_SOURCE_DIR}/include
      ${CMAKE_SOURCE_DIR}/vendor/unity
    )
    target_link_libraries(${tname} PRIVATE ssd-dbms)
    add_test(NAME ${tname} COMMAND ${tname})
  endforeach()
endif()
