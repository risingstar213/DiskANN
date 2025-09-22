// io_ring_wrapper.h
// 封装liburing相关API，便于扩展
#pragma once
#include <liburing.h>
#include <stdexcept>
#include <vector>
#include <sys/types.h>
#include "async_io.h"

// Forward declarations
struct IOAwaitable;
struct AlignedRead;

class IoRingWrapper : public AsyncIO {
public:
    IoRingWrapper(unsigned entries, unsigned flags = 0);
    ~IoRingWrapper();

    struct io_uring_sqe* get_sqe();
    int submit();
    int peek_cqe(struct io_uring_cqe** cqe);
    void cqe_seen(struct io_uring_cqe* cqe);
    struct io_uring* ring();
    void prep_read(struct io_uring_sqe* sqe, int fd, void* buf, size_t len, off_t offset);

    // Implements AsyncIO
    uint64_t add_read_request(int fd, void* buf, size_t len, off_t offset) override;
    void flush_batch() override; // 提交所有pending的read请求
    size_t pending_requests_count() const override;
    void clear_batch() override;
    std::vector<AsyncCompletion> poll_completions(int max_events) override;

private:
    struct io_uring ring_;
    uint64_t next_op_id = 1;
    
    struct PendingRead {
        int fd;
        void* buf;
        size_t len;
        off_t offset;
        uint64_t op_id;
    };
    std::vector<PendingRead> pending_reads_;
public:
};
