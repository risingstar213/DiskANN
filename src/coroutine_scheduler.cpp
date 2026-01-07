// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "coroutine_scheduler.h"
#include <atomic>
#include <coroutine>
#include <cstdio>
#include <iostream>
#include <system_error>
#include <cerrno>
#include <cstring>
#include <chrono>
#include <thread>
#include <unistd.h>
#include "io_ring_wrapper.h"
#include "libaio_wrapper.h"
#include "async_io.h"

namespace diskann {

std::unique_ptr<CoroutineScheduler> g_scheduler = nullptr;

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
    if (running.load()) {
        return;
    }
    running = true;
    io_thread_ = std::thread([this]() { io_thread_loop(); });
    compute_threads_.resize(5);
    for (size_t i = 0; i < compute_threads_.size(); ++i) {
        compute_threads_[i] = std::thread([this]() { compute_thread_loop(); });
    }
}

void CoroutineScheduler::run() {
    // 这里假设所有根协程已经被加入ready_queue_中，而且没有其他协程
    this->set_pending_cnts(ready_queue_.size());

    while (true) {
        // 检查是否所有根协程都完成
        if (pending_cnts_.load(std::memory_order_relaxed) == 0) {
            break;
        }

        usleep(100); // 避免忙等待
    }

    usleep(100); // 确保io线程处理完剩余的completion
}

void CoroutineScheduler::stop() {
    running = false;
    if (io_thread_.joinable()) {
        io_thread_.join();
    }

    for (auto& t : compute_threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
}

class YieldAwaitable {
public:
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> handle) noexcept;
    void await_resume() noexcept {}
};

void YieldAwaitable::await_suspend(std::coroutine_handle<> handle) noexcept {
    CoroutineScheduler* scheduler = get_cor_scheduler();
    if (scheduler) {
        scheduler->schedule_coroutine(handle);
    }
}

std::vector<IOAwaitable> CoroutineScheduler::async_read_batch(
    int fd,
    const std::vector<AlignedRead>& reads) {
    std::vector<IOAwaitable> awaitables;
    awaitables.reserve(reads.size());
    enqueue_read_requests(fd, reads, awaitables);

    return awaitables;
}

void CoroutineScheduler::schedule_coroutine(std::coroutine_handle<> coro) {
    if (!coro) {
        return;
    }
    std::lock_guard<std::mutex> lock(ready_mutex_);
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
                if (awaitable_ptr == nullptr) {
                    continue;
                }
                awaitable_ptr->result = result;
                uint8_t desired = IOAwaitable::Status::PENDING;
                if (!awaitable_ptr->status.compare_exchange_strong(desired, IOAwaitable::Status::COMPLETED)) {
                    awaitable_ptr->status.store(IOAwaitable::Status::COMPLETED, std::memory_order_release);
                    // 等待协程被设置
                    auto waiter = awaitable_ptr->waiting_coroutine.load(std::memory_order_relaxed);
                    while (waiter == std::coroutine_handle<>{}) {
                        waiter = awaitable_ptr->waiting_coroutine.load(std::memory_order_relaxed);
                    }
                    schedule_coroutine(waiter);
                }
            }
            pending_ops_.erase(it);
#else
            if (entry.remaining > 0) {
                entry.remaining--; // 减少待完成计数
            }
            if (entry.remaining == 0) {
                for (auto awaitable_ptr : entry.awaitables) {
                    if (awaitable_ptr == nullptr) {
                        continue;
                    }
                    awaitable_ptr->result = result;  // 所有awaitable使用最后一个result
                    uint8_t desired = IOAwaitable::Status::PENDING;
                    if (!awaitable_ptr->status.compare_exchange_strong(desired, IOAwaitable::Status::COMPLETED)) {
                        assert(desired == IOAwaitable::Status::WAITING);
                        awaitable_ptr->status.store(IOAwaitable::Status::COMPLETED, std::memory_order_release);
                        // 等待协程被设置
                        auto waiter = awaitable_ptr->waiting_coroutine.load(std::memory_order_relaxed);
                        while (waiter == std::coroutine_handle<>{}) {
                            waiter = awaitable_ptr->waiting_coroutine.load(std::memory_order_relaxed);
                        }
                        schedule_coroutine(waiter);
                    }
                }
                pending_ops_.erase(it);
            }
#endif
        }
    }
}

bool CoroutineScheduler::execute_ready_coroutines() {
    std::coroutine_handle<> coro;
    {
        std::lock_guard<std::mutex> lock(ready_mutex_);
        if (ready_queue_.empty()) {
            return false;
        }
        coro = ready_queue_.front();
        ready_queue_.pop();
    }

    // Resume the coroutine
    if (coro && !coro.done()) {
        coro.resume();
    }
    return true;
}

void CoroutineScheduler::io_thread_loop() {
    while (running) {
        drain_submission_queue();

        // 共享batch提交：io线程独立决定flush时机
        if (io_backend_->pending_requests_count() > 64) {
            io_backend_->flush_batch();
        }

        if (io_backend_->pending_requests_count() > 0 && ready_queue_.empty()) {
            io_backend_->flush_batch();
        }

        process_completions();
    }

    // Drain any remaining completions before exit
    io_backend_->flush_batch();
    process_completions();
}

void CoroutineScheduler::compute_thread_loop() {
    while (running) {
        if (pending_cnts_.load(std::memory_order_relaxed) == 0) {
            usleep(100); // 避免忙等待
            continue;
        }

        execute_ready_coroutines();
    }
}

void CoroutineScheduler::enqueue_read_requests(int fd, const std::vector<AlignedRead>& reads, std::vector<IOAwaitable>& awaitables) {
    awaitables.reserve(1);

    std::lock_guard<std::mutex> lock(submission_mutex_);
    for (size_t i = 0; i < reads.size(); ++i) {
        IOAwaitable* awaitable_ptr = nullptr;
        if (i == reads.size() - 1) {
            // 最后一个请求使用nullptr占位，避免多次拷贝
            awaitables.emplace_back(IOAwaitable{});
            awaitables.back().result = 0;
            awaitables.back().status.store(IOAwaitable::Status::PENDING, std::memory_order_relaxed);
            awaitables.back().waiting_coroutine.store(std::coroutine_handle<>{}, std::memory_order_relaxed);

            awaitable_ptr = &awaitables.back();
        }

        submission_queue_.push_back(PendingSubmission{fd, reads[i], awaitable_ptr});
    }
}

void CoroutineScheduler::drain_submission_queue() {
    uint64_t backend_cnts = io_backend_->pending_requests_count();
    std::vector<PendingSubmission> local_batch;
    {
        std::lock_guard<std::mutex> lock(submission_mutex_);
        while (!submission_queue_.empty() && backend_cnts + local_batch.size() <= 120) {
            local_batch.push_back(std::move(submission_queue_.front()));
            submission_queue_.pop_front();
        }
    }

    for (auto &req : local_batch) {
        uint64_t op_id = io_backend_->add_read_request(req.fd, req.read.buf, req.read.len, req.read.offset);

        auto &entry = pending_ops_[op_id];
        if (entry.awaitables.empty()) {
            entry.awaitables.reserve(64);
        }
        entry.awaitables.push_back(req.awaitable);

#if !defined(ENABLE_HITCHHIKE)
        entry.remaining += 1;  // 非HITCHHIKE模式：每个请求都增加计数
#else
        if (entry.remaining == 0) entry.remaining = 1; // HITCHHIKE: single completion
#endif
    }
}

} // namespace diskann
