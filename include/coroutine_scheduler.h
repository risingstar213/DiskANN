// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <coroutine>
#include <cstdint>
#include <functional>
#include <memory>
#include <queue>
#include <thread>
#include <atomic>
#include <stdexcept>
#include <mutex>
#include <deque>
#include "async_io.h"
#include "io_ring_wrapper.h"
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
    int result;
    std::atomic<uint8_t> status = Status::PENDING;
    std::atomic<std::coroutine_handle<>> waiting_coroutine{std::coroutine_handle<>()};

    enum Status : uint8_t {
        PENDING = 0,
        WAITING = 1,
        COMPLETED = 2
    };
    
    IOAwaitable() : result(0), status(PENDING), waiting_coroutine(std::coroutine_handle<>()) {}

    // copy
    IOAwaitable(const IOAwaitable& other) : result(other.result), status(other.status.load()), waiting_coroutine(std::coroutine_handle<>()) {}
    IOAwaitable(IOAwaitable &&other) : result(other.result), status(other.status.load()), waiting_coroutine(std::coroutine_handle<>()) {}
    
    bool await_ready() noexcept {
        uint8_t expected = PENDING;
        return !status.compare_exchange_strong(expected, WAITING);
    }
    void await_suspend(std::coroutine_handle<> h) noexcept {
        waiting_coroutine.store(h, std::memory_order_relaxed);
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

    // Schedule multiple async read operations
    std::vector<IOAwaitable> async_read_batch(
        int fd,
        const std::vector<AlignedRead>& reads
    );

    // Add a coroutine to the ready queue
    void schedule_coroutine(std::coroutine_handle<> coro);

    void set_pending_cnts(uint32_t cnt) {
        pending_cnts_.store(cnt, std::memory_order_relaxed);
    }

    // WARNING!!! 目前为了简化设计，应用手动设置和减少计数，之后可改为自动管理
    void mark_done() {
        pending_cnts_.fetch_sub(1, std::memory_order_relaxed);
    }

private:
    std::thread io_thread_;
    std::vector<std::thread> compute_threads_;
    std::unique_ptr<AsyncIO> io_backend_;
    std::atomic<bool> running{false};

    std::mutex ready_mutex_;
    std::queue<std::coroutine_handle<>> ready_queue_;
    // 合并pending_ops和pending_counts为一个结构
    struct PendingEntry {
        std::vector<IOAwaitable*> awaitables;
        size_t remaining = 0; // 待完成计数
    };
    std::unordered_map<uint64_t, PendingEntry> pending_ops_;  // op_id -> entry

    struct PendingSubmission {
        int fd = -1;
        AlignedRead read{};
        IOAwaitable* awaitable = nullptr;
    };

    // Simple bounded MPSC lock-free queue for submissions
    class SubmissionQueue {
    public:
        explicit SubmissionQueue(size_t capacity);
        bool enqueue(PendingSubmission&& item);
        bool dequeue(PendingSubmission& out);

    private:
        struct Slot {
            std::atomic<bool> ready{false};
            PendingSubmission value{};
        };

        const size_t capacity_;
        std::vector<Slot> buffer_;
        std::atomic<size_t> head_{0};
        std::atomic<size_t> tail_{0};
    };

    static constexpr size_t kSubmissionQueueCapacity = MAX_ENTRIES * 8;
    SubmissionQueue submission_queue_;

    // 跟踪协程执行
    std::atomic<uint32_t> pending_cnts_{0};

    void io_thread_loop();
    void compute_thread_loop();

    void enqueue_read_requests(int fd, const std::vector<AlignedRead>& reads, std::vector<IOAwaitable>& awaitables);
    void drain_submission_queue();

    void process_completions();
    bool execute_ready_coroutines();
};

// Thread-local scheduler instance for multi-threading isolation
extern std::unique_ptr<CoroutineScheduler> g_scheduler;

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
