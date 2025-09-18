// io_ring_wrapper.h
// 封装liburing相关API，便于扩展
#pragma once
#include <liburing.h>
#include <stdexcept>
#include <vector>

// Forward declarations
struct IOAwaitable;
struct AlignedRead;

class IoRingWrapper {
public:
    IoRingWrapper(unsigned entries, unsigned flags = 0);
    ~IoRingWrapper();

    struct io_uring_sqe* get_sqe();
    int submit();
    int peek_cqe(struct io_uring_cqe** cqe);
    void cqe_seen(struct io_uring_cqe* cqe);
    struct io_uring* ring();
    void prep_read(struct io_uring_sqe* sqe, int fd, void* buf, size_t len, off_t offset);

    // 批量read相关接口 - 支持多协程共享batch，但IOAwaitable私有
    void add_read_request(int fd, void* buf, size_t len, off_t offset, uint64_t op_id);
    void flush_batch(); // 提交所有pending的read请求
    size_t pending_requests_count() const;
    void clear_batch();

private:
    struct io_uring ring_;
    
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
