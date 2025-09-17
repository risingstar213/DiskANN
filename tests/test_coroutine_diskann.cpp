// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <iostream>
#include <chrono>
#include <vector>
#include <thread>
#include "coroutine_scheduler.h"
#include "async_linux_aligned_file_reader.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cstring>

// Simple test to verify coroutine scheduler and async file reader work
diskann::Task<void> test_basic_coroutine() {
    std::cout << "Starting basic coroutine test" << std::endl;
    
    // Create a test file
    std::string test_file = "/tmp/diskann_test_file.bin";
    
    // Write test data
    int fd = open(test_file.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd == -1) {
        std::cout << "Failed to create test file" << std::endl;
        co_return;
    }
    
    std::vector<char> test_data(4096, 'A');  // 4KB of 'A's
    write(fd, test_data.data(), test_data.size());
    close(fd);
    
    // Test async reading
    AsyncLinuxAlignedFileReader reader;
    reader.open(test_file);
    
    // Prepare aligned buffer
    void* aligned_buf;
    if (posix_memalign(&aligned_buf, 512, 4096) != 0) {
        std::cout << "Failed to allocate aligned memory" << std::endl;
        co_return;
    }
    
    std::vector<AlignedRead> reads;
    reads.emplace_back(0, 4096, aligned_buf);
    
    auto start = std::chrono::high_resolution_clock::now();
    
    try {
        co_await reader.async_read_coro(reads);
        auto end = std::chrono::high_resolution_clock::now();
        
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        std::cout << "Async read completed in " << duration.count() << " microseconds" << std::endl;
        
        // Verify data
        char* buf = static_cast<char*>(aligned_buf);
        bool correct = true;
        for (int i = 0; i < 4096; i++) {
            if (buf[i] != 'A') {
                correct = false;
                break;
            }
        }
        
        if (correct) {
            std::cout << "Data verification: SUCCESS" << std::endl;
        } else {
            std::cout << "Data verification: FAILED" << std::endl;
        }
        
    } catch (const std::exception& e) {
        std::cout << "Exception during async read: " << e.what() << std::endl;
    }
    
    free(aligned_buf);
    reader.close();
    unlink(test_file.c_str());
    
    std::cout << "Basic coroutine test completed" << std::endl;
    co_return;
}

diskann::Task<void> test_concurrent_reads() {
    std::cout << "Starting concurrent reads test" << std::endl;
    
    // Create test files
    std::vector<std::string> test_files;
    std::vector<AsyncLinuxAlignedFileReader> readers;
    
    for (int i = 0; i < 3; i++) {
        std::string test_file = "/tmp/diskann_test_file_" + std::to_string(i) + ".bin";
        test_files.push_back(test_file);
        
        // Write test data
        int fd = open(test_file.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
        std::vector<char> test_data(4096, 'A' + i);  // Different content for each file
        write(fd, test_data.data(), test_data.size());
        close(fd);
        
        readers.emplace_back();
        readers.back().open(test_file);
    }
    
    // Prepare aligned buffers
    std::vector<void*> aligned_bufs(3);
    for (int i = 0; i < 3; i++) {
        if (posix_memalign(&aligned_bufs[i], 512, 4096) != 0) {
            std::cout << "Failed to allocate aligned memory" << std::endl;
            co_return;
        }
    }
    
    auto start = std::chrono::high_resolution_clock::now();
    
    // Create concurrent read tasks
    std::vector<diskann::Task<void>> read_tasks;
    for (int i = 0; i < 3; i++) {
        std::vector<AlignedRead> reads;
        reads.emplace_back(0, 4096, aligned_bufs[i]);
        read_tasks.push_back(readers[i].async_read_coro(reads));
    }
    
    // Wait for all reads to complete
    for (auto& task : read_tasks) {
        co_await task;
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::cout << "Concurrent reads completed in " << duration.count() << " microseconds" << std::endl;
    
    // Verify data
    bool all_correct = true;
    for (int i = 0; i < 3; i++) {
        char* buf = static_cast<char*>(aligned_bufs[i]);
        char expected = 'A' + i;
        for (int j = 0; j < 4096; j++) {
            if (buf[j] != expected) {
                all_correct = false;
                break;
            }
        }
    }
    
    if (all_correct) {
        std::cout << "Concurrent data verification: SUCCESS" << std::endl;
    } else {
        std::cout << "Concurrent data verification: FAILED" << std::endl;
    }
    
    // Cleanup
    for (int i = 0; i < 3; i++) {
        free(aligned_bufs[i]);
        readers[i].close();
        unlink(test_files[i].c_str());
    }
    
    std::cout << "Concurrent reads test completed" << std::endl;
    co_return;
}

int main() {
    std::cout << "Starting DiskANN Coroutine Tests" << std::endl;
    
    // Initialize scheduler
    diskann::g_scheduler = std::make_unique<diskann::CoroutineScheduler>();
    diskann::g_scheduler->init();
    
    std::cout << "Scheduler initialized" << std::endl;
    
    bool test1_completed = false, test2_completed = false;
    
    // Create test coroutines
    auto test1 = test_basic_coroutine();
    auto test2 = test_concurrent_reads();
    
    // Schedule tests
    diskann::get_scheduler().schedule_coroutine(test1.coro);
    diskann::get_scheduler().schedule_coroutine(test2.coro);
    
    // Run scheduler in separate thread
    std::thread scheduler_thread([&]() {
        diskann::g_scheduler->run();
    });
    
    // Wait for tests to complete
    while ((!test1.coro || !test1.coro.done()) || 
           (!test2.coro || !test2.coro.done())) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    std::cout << "All tests completed" << std::endl;
    
    // Stop scheduler
    diskann::g_scheduler->stop();
    scheduler_thread.join();
    diskann::g_scheduler.reset();
    
    std::cout << "Tests finished successfully" << std::endl;
    return 0;
}
