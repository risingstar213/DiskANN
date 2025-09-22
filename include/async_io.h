// async_io.h
// Abstract async IO interface that unifies io_uring and libaio
#pragma once

#include <cstdint>
#include <vector>
#include <stdexcept>

struct AsyncCompletion {
    uint64_t op_id;
    int result;
};

class AsyncIO {
public:
    virtual ~AsyncIO() = default;

    // Submit a read request and return an op id (can be shared for batch)
    virtual uint64_t add_read_request(int fd, void* buf, size_t len, off_t offset) = 0;

    // Submit all pending requests
    virtual void flush_batch() = 0;

    // Number of pending requests in the current batch
    virtual size_t pending_requests_count() const = 0;

    // Clear the current batch without submitting
    virtual void clear_batch() = 0;

    // Poll up to max_events completions and return them as unified AsyncCompletion
    virtual std::vector<AsyncCompletion> poll_completions(int max_events) = 0;
};

#define ENABLE_HITCHHIKE
