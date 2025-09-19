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


CoroutineScheduler::CoroutineScheduler() : ring_wrapper_(MAX_ENTRIES, 0) {
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
        if (ready_queue.empty()) {
            if (pending_ops.empty()) {
                break; // Exit if no pending operations and no ready coroutines
            }
            // std::this_thread::sleep_for(std::chrono::microseconds(1));
        }

        // Periodically flush batched reads - 共享batch提交
        if (ring_wrapper_.pending_requests_count() >= 64 || ready_queue.empty()) {
            if (ring_wrapper_.pending_requests_count() > 0) {
                ring_wrapper_.flush_batch();
            }
        }
    }
}

void CoroutineScheduler::stop() {
    running = false;
}

IOAwaitable CoroutineScheduler::async_read(int fd, void* buf, size_t len, off_t offset) {
    IOAwaitable awaitable(nullptr);
    awaitable.result = 0;
    awaitable.ready = false;
    
    // 将单个请求添加到batch中，获取共享的op_id
    uint64_t op_id = ring_wrapper_.add_read_request(fd, buf, len, offset);
    
    // 将awaitable关联到op_id
    if (pending_ops.find(op_id) == pending_ops.end()) {
        pending_ops[op_id] = std::vector<IOAwaitable*>();
        pending_counts[op_id] = 0;  // 初始化计数器
    }
    pending_ops[op_id].push_back(&awaitable);
    
    // 累加计数器：每个awaitable在非HITCHHIKE模式下需要1个完成通知
    if (!ring_wrapper_.is_hitchhike_enabled()) {
        pending_counts[op_id]++;  // 非HITCHHIKE模式：每个awaitable累加1个完成通知
    } else {
        // HITCHHIKE模式：第一次设置为1，后续不增加（因为所有awaitable共享1个完成通知）
        if (pending_counts[op_id] == 0) {
            pending_counts[op_id] = 1;
        }
    }
    
    return awaitable;
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
    uint64_t shared_op_id = 0;
    for (size_t i = 0; i < reads.size(); ++i) {
        // 将请求添加到共享batch中，所有请求都会返回相同的op_id
        uint64_t op_id = ring_wrapper_.add_read_request(fd, reads[i].buf, reads[i].len, reads[i].offset);
        if (i == 0) {
            shared_op_id = op_id;  // 记录共享的op_id
        }
    }
    
    // 将所有awaitables关联到共享的op_id
    if (pending_ops.find(shared_op_id) == pending_ops.end()) {
        pending_ops[shared_op_id] = std::vector<IOAwaitable*>();
        pending_ops[shared_op_id].reserve(64);
        pending_counts[shared_op_id] = 0;  // 初始化计数器
    }
    
    for (auto& awaitable : awaitables) {
        pending_ops[shared_op_id].push_back(&awaitable);
    }
    
    // 累加计数器：考虑到可能有多个批次共用同一个op_id
    if (!ring_wrapper_.is_hitchhike_enabled()) {
        pending_counts[shared_op_id] += reads.size();  // 非HITCHHIKE模式：累加每个请求的完成通知
    } else {
        // HITCHHIKE模式：第一次设置为1，后续不增加（因为所有awaitable共享1个完成通知）
        if (pending_counts[shared_op_id] == 0) {
            pending_counts[shared_op_id] = 1;
        }
    }

    // ring_wrapper_.flush_batch();
    
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
            std::vector<IOAwaitable*>& awaitables = it->second;
            auto count_it = pending_counts.find(op_id);
            
            if (count_it == pending_counts.end()) {
                std::cerr << "Error: No pending count found for op_id: " << op_id << std::endl;
                ring_wrapper_.cqe_seen(cqe);
                continue;
            }
            
            if (ring_wrapper_.is_hitchhike_enabled()) {
                // HITCHHIKE模式：一个CQE完成所有相关的awaitables
                for (auto awaitable_ptr : awaitables) {
                    awaitable_ptr->result = result;
                    awaitable_ptr->ready = true;
                    if (awaitable_ptr->waiting_coroutine) {
                        schedule_coroutine(awaitable_ptr->waiting_coroutine);
                    }
                }
                // 立即删除，因为所有awaitables都已完成
                pending_ops.erase(it);
                pending_counts.erase(count_it);
            } else {
                // 非HITCHHIKE模式：只需要递减计数器，在全部完成时统一唤醒
                size_t& remaining_count = count_it->second;
                remaining_count--; // 减少待完成计数
                
                // 当所有IO都完成时，统一设置所有awaitables并唤醒协程（O(1)判断 + 批量处理）
                if (remaining_count == 0) {
                    for (auto awaitable_ptr : awaitables) {
                        awaitable_ptr->result = result;  // 所有awaitable使用最后一个result
                        awaitable_ptr->ready = true;
                        if (awaitable_ptr->waiting_coroutine) {
                            schedule_coroutine(awaitable_ptr->waiting_coroutine);
                        }
                    }
                    pending_ops.erase(it);
                    pending_counts.erase(count_it);
                }
            }
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
