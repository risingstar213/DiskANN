// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <coroutine>
#include <functional>
#include <memory>
#include <queue>
#include <thread>
#include <atomic>
#include <stdexcept>
#include <liburing.h>
#include <vector>
#include <unordered_map>
#include "aligned_file_reader.h"

namespace diskann {

// Forward declarations
class CoroutineScheduler;

// Task represents a coroutine that can be awaited
template<typename T = void>
struct Task {
    struct promise_type {
        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        // 惰性
        std::suspend_always initial_suspend() { return {}; }
        auto final_suspend() noexcept {
            struct Awaiter {
                bool await_ready() noexcept { return false; }
                void await_suspend(std::coroutine_handle<promise_type> h) noexcept {
                    if (h.promise().parent)
                        h.promise().parent.resume();  // 唤醒父协程
                }
                void await_resume() noexcept {}
            };
            return Awaiter{};
        }
        void return_value(T value) { result = std::move(value); }
        void unhandled_exception() { exception = std::current_exception(); }
        
        T result;
        std::coroutine_handle<> parent;  // ✅ 自定义字段，保存父协程句柄
        std::exception_ptr exception;
    };

    std::coroutine_handle<promise_type> coro;
    
    Task(std::coroutine_handle<promise_type> h) : coro(h) {}
    ~Task() { if (coro) coro.destroy(); }
    
    Task(Task&& other) noexcept : coro(std::exchange(other.coro, {})) {}
    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            if (coro) coro.destroy();
            coro = std::exchange(other.coro, {});
        }
        return *this;
    }
    
    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;
    
    T get_result() {
        if (coro.promise().exception) {
            std::rethrow_exception(coro.promise().exception);
        }
        return std::move(coro.promise().result);
    }
    
    // Make Task<T> awaitable
    bool await_ready() const noexcept { 
        return coro.done(); 
    }
    void await_suspend(std::coroutine_handle<> h) noexcept {
        // 父协程挂起，并保存句柄给子协程 final_suspend 来 resume
        coro.promise().parent = h;
        coro.resume();  // 启动子协程
    }
    T await_resume() {
        if (coro.promise().exception) {
            std::rethrow_exception(coro.promise().exception);
        }
        T result = std::move(coro.promise().result);
        return std::move(result);
    }
};

// Specialization for void
template<>
struct Task<void> {
    struct promise_type {
        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() { return {}; }
        auto final_suspend() noexcept {
            struct Awaiter {
                bool await_ready() noexcept { return false; }
                void await_suspend(std::coroutine_handle<promise_type> h) noexcept {
                    if (h.promise().parent)
                        h.promise().parent.resume();  // 唤醒父协程
                }
                void await_resume() noexcept {}
            };
            return Awaiter{};
        }
        void return_void() {}
        void unhandled_exception() { exception = std::current_exception(); }
        
        std::coroutine_handle<> parent;  // ✅ 自定义字段，保存父协程句柄
        std::exception_ptr exception;
    };

    std::coroutine_handle<promise_type> coro;
    
    Task(std::coroutine_handle<promise_type> h) : coro(h) {}
    ~Task() { if (coro) coro.destroy(); }
    
    Task(Task&& other) noexcept : coro(std::exchange(other.coro, {})) {}
    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            if (coro) coro.destroy();
            coro = std::exchange(other.coro, {});
        }
        return *this;
    }
    
    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;
    
    void get_result() {
        if (coro.promise().exception) {
            std::rethrow_exception(coro.promise().exception);
        }
    }
    
    // Make Task<void> awaitable
    bool await_ready() const noexcept { 
        return coro.done(); 
    }
    void await_suspend(std::coroutine_handle<> h) noexcept {
        // 父协程挂起，并保存句柄给子协程 final_suspend 来 resume
        coro.promise().parent = h;
        coro.resume();  // 启动子协程
    }
    void await_resume() const {
        if (coro.promise().exception) {
            std::rethrow_exception(coro.promise().exception);
        }
        return;
    }
};

// Awaitable for IO operations
struct IOAwaitable {
    struct io_uring_sqe* sqe;
    int result;
    bool ready = false;
    std::coroutine_handle<> waiting_coroutine;
    
    IOAwaitable(struct io_uring_sqe* s) : sqe(s) {}
    
    bool await_ready() const noexcept { return ready; }
    void await_suspend(std::coroutine_handle<> h) noexcept {
        waiting_coroutine = h;
    }
    int await_resume() const noexcept {
        return result;
    }
};

// Coroutine scheduler using io_uring
class CoroutineScheduler {
public:
    static constexpr size_t MAX_ENTRIES = 1024;
    
    CoroutineScheduler();
    ~CoroutineScheduler();
    
    // Initialize the scheduler
    void init();
    
    // Run the event loop
    void run();
    
    // Stop the scheduler
    void stop();
    
    // Schedule an async read operation
    IOAwaitable async_read(int fd, void* buf, size_t len, off_t offset);
    
    // Schedule multiple async read operations
    std::vector<IOAwaitable> async_read_batch(
        int fd, 
        const std::vector<AlignedRead>& reads
    );
    
    // Add a coroutine to the ready queue
    void schedule_coroutine(std::coroutine_handle<> coro);
    
private:
    struct io_uring ring;
    std::atomic<bool> running{false};
    std::queue<std::coroutine_handle<>> ready_queue;
    std::unordered_map<uint64_t, IOAwaitable*> pending_ops;
    std::mutex ready_mutex;
    uint64_t next_op_id = 1;
    
    void process_completions();
    void execute_ready_coroutines();
};

// Thread-local scheduler instance for multi-threading isolation
extern thread_local std::unique_ptr<CoroutineScheduler> g_scheduler;

// Helper to get the scheduler
inline CoroutineScheduler* get_cor_scheduler() {
    if (!g_scheduler) {
        throw std::runtime_error("Scheduler not available - call init_scheduler() first");
    }
    return g_scheduler.get();
}

// Helper to initialize the scheduler for the current thread
inline void init_scheduler() {
    if (!g_scheduler) {
        g_scheduler = std::make_unique<CoroutineScheduler>();
        g_scheduler->init();
    }
}

} // namespace diskann
