# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT license.

# CMakeLists.txt additions for coroutine support

# Add coroutine support (requires C++20)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS "10.0")
        message(FATAL_ERROR "GCC 10.0 or later required for coroutines")
    endif()
    set(COROUTINE_FLAGS "-std=c++20 -fcoroutines")
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS "14.0")
        message(FATAL_ERROR "Clang 14.0 or later required for coroutines")
    endif()
    set(COROUTINE_FLAGS "-std=c++20 -stdlib=libc++")
else()
    message(WARNING "Compiler may not support C++20 coroutines")
    set(COROUTINE_FLAGS "-std=c++20")
endif()

# Add io_uring library for Linux
if(NOT MSVC AND NOT APPLE)
    find_library(LIBURING_LIB uring)
    if(NOT LIBURING_LIB)
        message(FATAL_ERROR "liburing not found. Please install liburing-dev")
    endif()
    set(ASYNC_LIBS ${LIBURING_LIB})
endif()

# Add coroutine source files to the main library
if(NOT MSVC)
    set(ASYNC_CPP_SOURCES 
        src/coroutine_scheduler.cpp
        src/async_linux_aligned_file_reader.cpp
        src/async_pq_flash_index.cpp
    )
    
    # Add to existing sources
    list(APPEND CPP_SOURCES ${ASYNC_CPP_SOURCES})
endif()

# Create async search executable
if(NOT MSVC)
    add_executable(async_search_disk_index apps/async_search_disk_index.cpp)
    target_compile_options(async_search_disk_index PRIVATE ${COROUTINE_FLAGS})
    target_link_libraries(async_search_disk_index ${PROJECT_NAME} ${DISKANN_ASYNC_LIB} ${ASYNC_LIBS} ${DISKANN_TOOLS_TCMALLOC_LINK_OPTIONS} Boost::program_options)
    
    # Create test executable
    add_executable(test_coroutine_diskann tests/test_coroutine_diskann.cpp)
    target_compile_options(test_coroutine_diskann PRIVATE ${COROUTINE_FLAGS})
    target_link_libraries(test_coroutine_diskann ${PROJECT_NAME} ${ASYNC_LIBS})
endif()

# Set C++20 standard for the main library if we're building async version
if(NOT MSVC AND ASYNC_LIBS)
    set_property(TARGET ${PROJECT_NAME} PROPERTY CXX_STANDARD 20)
    set_property(TARGET ${PROJECT_NAME}_s PROPERTY CXX_STANDARD 20)
    target_compile_options(${PROJECT_NAME} PRIVATE ${COROUTINE_FLAGS})
    target_compile_options(${PROJECT_NAME}_s PRIVATE ${COROUTINE_FLAGS})
    target_link_libraries(${PROJECT_NAME} ${ASYNC_LIBS})
    target_link_libraries(${PROJECT_NAME}_s ${ASYNC_LIBS})
endif()

# Install async executable
if(NOT MSVC AND ASYNC_LIBS)
    install(TARGETS async_search_disk_index test_coroutine_diskann RUNTIME)
endif()
