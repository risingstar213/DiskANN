# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT license.

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_COMPILE_WARNING_AS_ERROR ON)

add_executable(build_memory_index build_memory_index.cpp)
target_link_libraries(build_memory_index ${PROJECT_NAME} ${DISKANN_TOOLS_TCMALLOC_LINK_OPTIONS} Boost::program_options)

add_executable(build_stitched_index build_stitched_index.cpp)
target_link_libraries(build_stitched_index ${PROJECT_NAME} ${DISKANN_TOOLS_TCMALLOC_LINK_OPTIONS} Boost::program_options)

add_executable(search_memory_index search_memory_index.cpp)
target_link_libraries(search_memory_index ${PROJECT_NAME} ${DISKANN_ASYNC_LIB} ${DISKANN_TOOLS_TCMALLOC_LINK_OPTIONS} Boost::program_options)

add_executable(build_disk_index build_disk_index.cpp)
target_link_libraries(build_disk_index ${PROJECT_NAME} ${DISKANN_TOOLS_TCMALLOC_LINK_OPTIONS} ${DISKANN_ASYNC_LIB} Boost::program_options)

add_executable(search_disk_index search_disk_index.cpp)
target_link_libraries(search_disk_index ${PROJECT_NAME} ${DISKANN_ASYNC_LIB} ${DISKANN_TOOLS_TCMALLOC_LINK_OPTIONS} Boost::program_options)

add_executable(range_search_disk_index range_search_disk_index.cpp)
target_link_libraries(range_search_disk_index ${PROJECT_NAME} ${DISKANN_ASYNC_LIB} ${DISKANN_TOOLS_TCMALLOC_LINK_OPTIONS} Boost::program_options)

add_executable(test_streaming_scenario test_streaming_scenario.cpp)
target_link_libraries(test_streaming_scenario ${PROJECT_NAME} ${DISKANN_TOOLS_TCMALLOC_LINK_OPTIONS} Boost::program_options)

add_executable(test_insert_deletes_consolidate test_insert_deletes_consolidate.cpp)
target_link_libraries(test_insert_deletes_consolidate ${PROJECT_NAME} ${DISKANN_TOOLS_TCMALLOC_LINK_OPTIONS} Boost::program_options)

add_executable(socket_disk_search socket_disk_search.cpp)
target_link_libraries(socket_disk_search ${PROJECT_NAME} ${DISKANN_ASYNC_LIB} ${DISKANN_TOOLS_TCMALLOC_LINK_OPTIONS} Boost::program_options)

# Add async search executable if coroutines are enabled
if(ENABLE_COROUTINES AND NOT MSVC)
    add_executable(async_search_disk_index async_search_disk_index.cpp)
    set_property(TARGET async_search_disk_index PROPERTY CXX_STANDARD 20)
    if(COROUTINE_FLAGS)
        target_compile_options(async_search_disk_index PRIVATE ${COROUTINE_FLAGS})
    endif()
    target_link_libraries(async_search_disk_index ${PROJECT_NAME} ${DISKANN_ASYNC_LIB} ${LIBURING_LIB} ${DISKANN_TOOLS_TCMALLOC_LINK_OPTIONS} Boost::program_options)

    add_executable(async_socket_disk_search async_socket_disk_search.cpp)
    set_property(TARGET async_socket_disk_search PROPERTY CXX_STANDARD 20)
    if(COROUTINE_FLAGS)
        target_compile_options(async_socket_disk_search PRIVATE ${COROUTINE_FLAGS})
    endif()
    target_link_libraries(async_socket_disk_search ${PROJECT_NAME} ${DISKANN_ASYNC_LIB} ${LIBURING_LIB} ${DISKANN_TOOLS_TCMALLOC_LINK_OPTIONS} Boost::program_options)
    
    # Add simple async search program if coroutines are enabled
    add_executable(simple_async_search simple_async_search.cpp)
    if(COROUTINE_FLAGS)
        target_compile_options(simple_async_search PRIVATE ${COROUTINE_FLAGS})
    endif()
    target_link_libraries(simple_async_search ${PROJECT_NAME} ${DISKANN_ASYNC_LIB} ${LIBURING_LIB} ${DISKANN_TOOLS_TCMALLOC_LINK_OPTIONS} Boost::program_options)

    # Add test executable
    add_executable(test_coroutine_diskann test_coroutine_diskann.cpp)
    set_property(TARGET test_coroutine_diskann PROPERTY CXX_STANDARD 20)
    if(COROUTINE_FLAGS)
        target_compile_options(test_coroutine_diskann PRIVATE ${COROUTINE_FLAGS})
    endif()
    target_link_libraries(test_coroutine_diskann ${PROJECT_NAME} ${LIBURING_LIB})
endif()

# Add standalone demo (works without liburing/full setup)
add_executable(demo_coroutine_diskann demo_coroutine_diskann.cpp)
set_property(TARGET demo_coroutine_diskann PROPERTY CXX_STANDARD 20)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if(CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "10.0")
        target_compile_options(demo_coroutine_diskann PRIVATE -fcoroutines)
    endif()
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang") 
    if(CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "14.0")
        target_compile_options(demo_coroutine_diskann PRIVATE -stdlib=libc++)
    endif()
endif()
target_link_libraries(demo_coroutine_diskann pthread)

if (NOT MSVC)
    set(INSTALL_TARGETS build_memory_index
            build_stitched_index
            search_memory_index
            build_disk_index
            search_disk_index
            range_search_disk_index
            test_streaming_scenario
            test_insert_deletes_consolidate
            demo_coroutine_diskann)
    
    # Add async targets to install if available
    if(ENABLE_COROUTINES)
        list(APPEND INSTALL_TARGETS async_search_disk_index async_socket_disk_search test_coroutine_diskann simple_async_search)
    endif()
    
    install(TARGETS ${INSTALL_TARGETS} RUNTIME)
endif()
