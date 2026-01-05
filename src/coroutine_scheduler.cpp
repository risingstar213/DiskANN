// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "coroutine_scheduler.h"
#include <iostream>
#include <system_error>
#include <cerrno>
#include <cstring>
#include <chrono>
#include "io_ring_wrapper.h"
#include "libaio_wrapper.h"
#include "async_io.h"

namespace diskann {

thread_local std::unique_ptr<CoroutineScheduler> g_scheduler;


CoroutineScheduler::CoroutineScheduler() {
    // Default backend: io_uring
    io_backend_ = std::make_unique<IoRingWrapper>(MAX_ENTRIES, 0);
    // io_backend_ = std::make_unique<LibAioWrapper>(MAX_ENTRIES);
}


CoroutineScheduler::~CoroutineScheduler() {
    stop();
    // ring_wrapper_析构自动清理
}

void CoroutineScheduler::init() {
    // 已在IoRingWrapper构造时初始化
}

void CoroutineScheduler::run() {
    running = true;

    while (running) {
        // Process IO completions
        process_completions();

        // Execute ready coroutines
        execute_ready_coroutines();

        // Small yield to prevent busy waiting
        if (ready_queue_.empty()) {
            if (pending_ops_.empty()) {
                break; // Exit if no pending operations and no ready coroutines
            }
        }

        // Periodically flush batched reads - 共享batch提交
        if (io_backend_->pending_requests_count() >= 64 || ready_queue_.empty()) {
            if (io_backend_->pending_requests_count() > 0) {
                io_backend_->flush_batch();
            }
        }
    }
}

void CoroutineScheduler::stop() {
    running = false;
}

std::vector<IOAwaitable> CoroutineScheduler::async_read_batch(
    int fd,
    const std::vector<AlignedRead>& reads) {
    std::vector<IOAwaitable> awaitables;
    awaitables.reserve(reads.size());
    
    // 创建所有私有的awaitables
    for (size_t i = 0; i < reads.size(); ++i) {
        awaitables.emplace_back(nullptr);
        awaitables.back().result = 0;
        awaitables.back().ready = false;
    }
    
    // 粗粒度收割：所有请求共享一个op_id
    for (size_t i = 0; i < reads.size(); ++i) {
        uint64_t op_id = io_backend_->add_read_request(fd, reads[i].buf, reads[i].len, reads[i].offset);

        auto &entry = pending_ops_[op_id];
        if (entry.awaitables.empty()) entry.awaitables.reserve(64);
        entry.awaitables.push_back(&awaitables[i]);

#if !defined(ENABLE_HITCHHIKE)
        entry.remaining += 1;  // 非HITCHHIKE模式：每个请求都增加计数
#else
        if (entry.remaining == 0) entry.remaining = 1; // HITCHHIKE: single completion
#endif

        if (entry.awaitables.size() >= 120) {
            printf("[CoroutineScheduler] Warning: Large batch size for op_id %lu: %zu\n",
                   op_id, entry.awaitables.size());
            io_backend_->flush_batch(); // 防止计数过大
        }
    }

    // io_backend_ flush deferred to scheduler loop
    
    return awaitables;
}

void CoroutineScheduler::schedule_coroutine(std::coroutine_handle<> coro) {
    // std::lock_guard<std::mutex> lock(ready_mutex_);
    ready_queue_.push(coro);
}

void CoroutineScheduler::process_completions() {
    // Poll completions from backend
    auto completions = io_backend_->poll_completions(128);
    for (const auto &ac : completions) {
        uint64_t op_id = ac.op_id;
        int result = ac.result;
        auto it = pending_ops_.find(op_id);
        if (it != pending_ops_.end()) {
            auto &entry = it->second;
#ifdef ENABLE_HITCHHIKE
            for (auto awaitable_ptr : entry.awaitables) {
                awaitable_ptr->result = result;
                awaitable_ptr->ready = true;
                if (awaitable_ptr->waiting_coroutine) {
                    schedule_coroutine(awaitable_ptr->waiting_coroutine);
                }
            }
            pending_ops_.erase(it);
#else
            if (entry.remaining > 0) {
                entry.remaining--; // 减少待完成计数
            }
            if (entry.remaining == 0) {
                for (auto awaitable_ptr : entry.awaitables) {
                    awaitable_ptr->result = result;  // 所有awaitable使用最后一个result
                    awaitable_ptr->ready = true;
                    if (awaitable_ptr->waiting_coroutine) {
                        schedule_coroutine(awaitable_ptr->waiting_coroutine);
                    }
                }
                pending_ops_.erase(it);
            }
#endif
        }
    }
}

void CoroutineScheduler::execute_ready_coroutines() {
    // std::lock_guard<std::mutex> lock(ready_mutex);
    
    if (!ready_queue_.empty()) {
        auto coro = ready_queue_.front();
        ready_queue_.pop();
        
        // Resume the coroutine
        if (coro && !coro.done()) {
            coro.resume();
        }
    }
}

} // namespace diskann
