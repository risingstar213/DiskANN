#Copyright(c) Microsoft Corporation.All rights reserved.
#Licensed under the MIT                        license.

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_COMPILE_WARNING_AS_ERROR ON)

# Check for coroutine support and add async sources
option(ENABLE_COROUTINES "Enable coroutine-based async DiskANN" OFF)

if(ENABLE_COROUTINES AND NOT MSVC)
    # Set C++20 standard for coroutine support
    set(CMAKE_CXX_STANDARD 20)
    
    # Check compiler support
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS "10.0")
            message(FATAL_ERROR "GCC 10.0 or later required for coroutines")
        endif()
        set(COROUTINE_FLAGS "-fcoroutines")
    elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS "14.0")
            message(FATAL_ERROR "Clang 14.0 or later required for coroutines")
        endif()
        set(COROUTINE_FLAGS "-stdlib=libc++")
    endif()
    
    # Find liburing
    find_library(LIBURING_LIB uring)
    if(NOT LIBURING_LIB)
        message(FATAL_ERROR "liburing not found. Please install liburing-dev")
    endif()

    # Find libaio
    find_library(LIBAIO_LIB aio)
    if(NOT LIBAIO_LIB)
        message(FATAL_ERROR "libaio not found. Please install libaio-dev")
    endif()
    
    message(STATUS "Coroutine support enabled with liburing: ${LIBURING_LIB}")
endif()

if(MSVC)
    add_subdirectory(dll)
else()
    #file(GLOB CPP_SOURCES *.cpp)
    set(CPP_SOURCES abstract_data_store.cpp ann_exception.cpp disk_utils.cpp 
        distance.cpp index.cpp in_mem_graph_store.cpp in_mem_data_store.cpp
        linux_aligned_file_reader.cpp math_utils.cpp natural_number_map.cpp
        in_mem_data_store.cpp in_mem_graph_store.cpp
        natural_number_set.cpp memory_mapper.cpp partition.cpp pq.cpp
        pq_flash_index.cpp scratch.cpp logger.cpp utils.cpp filter_utils.cpp index_factory.cpp abstract_index.cpp pq_l2_distance.cpp pq_data_store.cpp)
    
    # Add async sources if coroutines are enabled
    if(ENABLE_COROUTINES)
        list(APPEND CPP_SOURCES 
            coroutine_scheduler.cpp
            async_linux_aligned_file_reader.cpp
            async_pq_flash_index.cpp
            io_ring_wrapper.cpp
            libaio_wrapper.cpp
        )
    endif()
    
    if (RESTAPI)
        list(APPEND CPP_SOURCES restapi/search_wrapper.cpp restapi/server.cpp)
    endif()
    
    add_library(${PROJECT_NAME} ${CPP_SOURCES})
    add_library(${PROJECT_NAME}_s STATIC ${CPP_SOURCES})
    
    # Add coroutine compiler flags and link liburing
    if(ENABLE_COROUTINES)
        target_compile_options(${PROJECT_NAME} PRIVATE ${COROUTINE_FLAGS})
        target_compile_options(${PROJECT_NAME}_s PRIVATE ${COROUTINE_FLAGS})
        target_link_libraries(${PROJECT_NAME} ${LIBURING_LIB} ${LIBAIO_LIB})
        target_link_libraries(${PROJECT_NAME}_s ${LIBURING_LIB} ${LIBAIO_LIB})
        target_compile_definitions(${PROJECT_NAME} PRIVATE DISKANN_ASYNC_ENABLED)
        target_compile_definitions(${PROJECT_NAME}_s PRIVATE DISKANN_ASYNC_ENABLED)
    endif()
endif()

if (NOT MSVC)
    install(TARGETS ${PROJECT_NAME} LIBRARY)
endif()
