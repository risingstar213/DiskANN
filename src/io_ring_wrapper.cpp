#include <liburing.h>
#include "io_ring_wrapper.h"
#include "aligned_file_reader.h" // for AlignedRead

#define ENABLE_HITCHHIKE

IoRingWrapper::IoRingWrapper(unsigned entries, unsigned flags) {
#ifdef ENABLE_HITCHHIKE
    // 启用Hitchhike优化
    flags |= IORING_SETUP_HIT;
#endif
    int ret = io_uring_queue_init(entries, &ring_, flags);
    if (ret < 0) throw std::runtime_error("io_uring_queue_init failed");
}

IoRingWrapper::~IoRingWrapper() {
    io_uring_queue_exit(&ring_);
}

struct io_uring_sqe* IoRingWrapper::get_sqe() {
    return io_uring_get_sqe(&ring_);
}

int IoRingWrapper::submit() {
    return io_uring_submit(&ring_);
}

int IoRingWrapper::peek_cqe(struct io_uring_cqe** cqe) {
    return io_uring_peek_cqe(&ring_, cqe);
}

void IoRingWrapper::cqe_seen(struct io_uring_cqe* cqe) {
    io_uring_cqe_seen(&ring_, cqe);
}

struct io_uring* IoRingWrapper::ring() {
    return &ring_;
}

void IoRingWrapper::prep_read(struct io_uring_sqe* sqe, int fd, void* buf, size_t len, off_t offset) {
    io_uring_prep_read(sqe, fd, buf, len, offset);
}

uint64_t IoRingWrapper::add_read_request(int fd, void* buf, size_t len, off_t offset) {
    uint64_t current_op_id = next_op_id;
#ifndef ENABLE_HITCHHIKE
    next_op_id++;
#endif
    pending_reads_.push_back(PendingRead{fd, buf, len, offset, current_op_id});
    return current_op_id;
}

void IoRingWrapper::flush_batch() {
    if (pending_reads_.empty()) return;

    assert(pending_reads_.size() < 96); // sanity check

#ifdef ENABLE_HITCHHIKE
    struct io_uring_sqe* sqe = get_sqe();
    uint32_t sqe_index = sqe - ring_.sq.sqes;
    if (ring_.hites == nullptr) {
        printf("ring_.hites is null\n");
        throw std::runtime_error("Hitchhike not initialized in io_uring");
    }
    // reverse
    // std::reverse(pending_reads_.begin(), pending_reads_.end());

    // 只提交第一个请求，其他的通过Hitchhike传递
    PendingRead* req = &pending_reads_[0];
    io_uring_prep_read(sqe, req->fd, req->buf, 4096, req->offset);
    sqe->user_data = next_op_id++;

    if (pending_reads_.size() > 1) {
        struct hitchhiker* hit = &ring_.hites[sqe_index];
        hit->max = pending_reads_.size() - 2;
        hit->in_use = 1;
        hit->iov_use = 1;
        hit->size = 4096; // page size
        for (uint32_t i = 1; i < pending_reads_.size(); ++i) {
            hit->addr[i-1] = (uint64_t)pending_reads_[i].offset;
            hit->iov[i-1] = (uint64_t)pending_reads_[i].buf;
        }
        sqe->flags |= IOSQE_HIT; // 标记为Hitchhike请求
    }
#else
    for (const auto& req : pending_reads_) {
        struct io_uring_sqe* sqe = get_sqe();
        if (!sqe) throw std::runtime_error("Failed to get SQE for read");
        
        io_uring_prep_read(sqe, req.fd, req.buf, req.len, req.offset);
        sqe->user_data = req.op_id;
    }
#endif
    submit();
    pending_reads_.clear();
}

size_t IoRingWrapper::pending_requests_count() const {
    return pending_reads_.size();
}

void IoRingWrapper::clear_batch() {
    pending_reads_.clear();
}
