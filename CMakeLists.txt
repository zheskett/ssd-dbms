cmake_minimum_required(VERSION 3.16)
project(ssd-dbms C)
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

add_compile_options(-Wall -Werror)

# Build the static library
file(GLOB_RECURSE SRC CONFIGURE_DEPENDS "${CMAKE_SOURCE_DIR}/src/*.c")
list(FILTER SRC EXCLUDE REGEX ".*/src/main\\.c$")
add_library(ssd-dbms STATIC ${SRC})
target_include_directories(ssd-dbms PUBLIC  ${CMAKE_SOURCE_DIR}/include PRIVATE ${CMAKE_SOURCE_DIR}/src)

# Build the CLI executable
add_executable(ssd-dbms-cli ${CMAKE_SOURCE_DIR}/src/main.c)
target_link_libraries(ssd-dbms-cli PRIVATE ssd-dbms)
target_include_directories(ssd-dbms-cli PRIVATE ${CMAKE_SOURCE_DIR}/include)

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
