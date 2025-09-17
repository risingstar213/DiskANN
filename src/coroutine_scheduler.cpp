// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "coroutine_scheduler.h"
#include <iostream>
#include <system_error>
#include <cerrno>
#include <cstring>
#include <chrono>

namespace diskann {

std::unique_ptr<CoroutineScheduler> g_scheduler;

CoroutineScheduler::CoroutineScheduler() {
    memset(&ring, 0, sizeof(ring));
}

CoroutineScheduler::~CoroutineScheduler() {
    stop();
    io_uring_queue_exit(&ring);
}

void CoroutineScheduler::init() {
    int ret = io_uring_queue_init(MAX_ENTRIES, &ring, 0);
    if (ret < 0) {
        throw std::system_error(-ret, std::system_category(), "Failed to init io_uring");
    }
}

void CoroutineScheduler::run() {
    running = true;
    
    while (running) {
        // Process IO completions
        process_completions();
        
        // Execute ready coroutines
        execute_ready_coroutines();
        
        // Small yield to prevent busy waiting
        if (ready_queue.empty()) {
            std::this_thread::sleep_for(std::chrono::microseconds(1));
        }
    }
}

void CoroutineScheduler::stop() {
    running = false;
}

IOAwaitable CoroutineScheduler::async_read(int fd, void* buf, size_t len, off_t offset) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
    if (!sqe) {
        throw std::runtime_error("Failed to get SQE");
    }
    
    // Prepare read operation
    io_uring_prep_read(sqe, fd, buf, len, offset);
    
    // Create awaitable with unique operation ID
    uint64_t op_id = next_op_id++;
    sqe->user_data = op_id;
    
    IOAwaitable awaitable(sqe);
    pending_ops[op_id] = &awaitable;
    
    // Submit the operation
    io_uring_submit(&ring);
    
    return awaitable;
}

std::vector<IOAwaitable> CoroutineScheduler::async_read_batch(
    int fd, 
    const std::vector<AlignedRead>& reads) {
    
    std::vector<IOAwaitable> awaitables;
    awaitables.reserve(reads.size());
    
    for (const auto& read : reads) {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
        if (!sqe) {
            throw std::runtime_error("Failed to get SQE");
        }
        
        // Prepare read operation
        io_uring_prep_read(sqe, fd, read.buf, read.len, read.offset);
        
        // Create awaitable with unique operation ID
        uint64_t op_id = next_op_id++;
        sqe->user_data = op_id;
        
        awaitables.emplace_back(sqe);
        pending_ops[op_id] = &awaitables.back();
    }
    
    // Submit all operations
    io_uring_submit(&ring);
    
    return awaitables;
}

void CoroutineScheduler::schedule_coroutine(std::coroutine_handle<> coro) {
    std::lock_guard<std::mutex> lock(ready_mutex);
    ready_queue.push(coro);
}

void CoroutineScheduler::process_completions() {
    struct io_uring_cqe* cqe;
    int ret;
    
    // Process all available completions
    while ((ret = io_uring_peek_cqe(&ring, &cqe)) == 0) {
        uint64_t op_id = cqe->user_data;
        int result = cqe->res;
        
        // Find the corresponding awaitable
        auto it = pending_ops.find(op_id);
        if (it != pending_ops.end()) {
            IOAwaitable* awaitable = it->second;
            awaitable->result = result;
            awaitable->ready = true;
            
            // Resume the waiting coroutine
            if (awaitable->waiting_coroutine) {
                schedule_coroutine(awaitable->waiting_coroutine);
            }
            
            pending_ops.erase(it);
        }
        
        // Mark completion as seen
        io_uring_cqe_seen(&ring, cqe);
    }
}

void CoroutineScheduler::execute_ready_coroutines() {
    std::lock_guard<std::mutex> lock(ready_mutex);
    
    while (!ready_queue.empty()) {
        auto coro = ready_queue.front();
        ready_queue.pop();
        
        // Resume the coroutine
        if (coro && !coro.done()) {
            coro.resume();
        }
    }
}

} // namespace diskann
