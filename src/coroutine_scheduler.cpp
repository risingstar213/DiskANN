// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "coroutine_scheduler.h"
#include <iostream>
#include <system_error>
#include <cerrno>
#include <cstring>
#include <chrono>
#include "io_ring_wrapper.h"

namespace diskann {

thread_local std::unique_ptr<CoroutineScheduler> g_scheduler;


CoroutineScheduler::CoroutineScheduler() : ring_wrapper_(MAX_ENTRIES, IORING_SETUP_SQPOLL) {
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
    int loop_cnt = 0;

    while (running) {
        // Process IO completions
        process_completions();

        // Execute ready coroutines
        execute_ready_coroutines();

        // Small yield to prevent busy waiting
        if (ready_queue.empty()) {
            if (pending_ops.empty()) {
                break; // Exit if no pending operations and no ready coroutines
            }
            // std::this_thread::sleep_for(std::chrono::microseconds(1));
        }

        // Periodically flush batched reads - 共享batch提交
        // if (loop_cnt % 8 == 0 || ready_queue.empty()) {
        //     if (ring_wrapper_.pending_requests_count() > 0) {
        //         ring_wrapper_.flush_batch();
        //     }
        // }
        ++loop_cnt;
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
    
    // 为每个read创建私有的awaitable和op_id
    for (size_t i = 0; i < reads.size(); ++i) {
        uint64_t op_id = next_op_id++;
        awaitables.emplace_back(nullptr);
        awaitables.back().result = 0;
        awaitables.back().ready = false;
        pending_ops[op_id] = &awaitables.back();
        
        // 将请求添加到共享batch中，而不是立即提交
        ring_wrapper_.add_read_request(fd, reads[i].buf, reads[i].len, reads[i].offset, op_id);
    }

    ring_wrapper_.flush_batch();
    
    // 如果batch足够大，立即flush；否则等待周期性flush
    // const size_t BATCH_FLUSH_THRESHOLD = 16;
    // if (ring_wrapper_.pending_requests_count() >= BATCH_FLUSH_THRESHOLD) {
    //     ring_wrapper_.flush_batch();
    // }
    
    return awaitables;
}

void CoroutineScheduler::schedule_coroutine(std::coroutine_handle<> coro) {
    // std::lock_guard<std::mutex> lock(ready_mutex);
    ready_queue.push(coro);
}

void CoroutineScheduler::process_completions() {
    struct io_uring_cqe* cqe;
    int ret;
    while ((ret = ring_wrapper_.peek_cqe(&cqe)) == 0) {
        uint64_t op_id = cqe->user_data;
        int result = cqe->res;
        auto it = pending_ops.find(op_id);
        if (it != pending_ops.end()) {
            IOAwaitable* awaitable = it->second;
            awaitable->result = result;
            awaitable->ready = true;
            if (awaitable->waiting_coroutine) {
                schedule_coroutine(awaitable->waiting_coroutine);
            }
            pending_ops.erase(it);
        }
        ring_wrapper_.cqe_seen(cqe);
    }
}

void CoroutineScheduler::execute_ready_coroutines() {
    // std::lock_guard<std::mutex> lock(ready_mutex);
    
    if (!ready_queue.empty()) {
        auto coro = ready_queue.front();
        ready_queue.pop();
        
        // Resume the coroutine
        if (coro && !coro.done()) {
            coro.resume();
        }
    }
}

} // namespace diskann
