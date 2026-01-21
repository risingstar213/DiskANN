#include <liburing.h>
#include <liburing/io_uring.h>
#include "io_ring_wrapper.h"
#include "aligned_file_reader.h" // for AlignedRead
#include "async_io.h"

IoRingWrapper::IoRingWrapper(unsigned entries, unsigned flags) {
    if (hitchhike_enabled()) {
        // 启用Hitchhike优化
        flags |= IORING_SETUP_HIT | IORING_SETUP_IOPOLL;
    }
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
    // 不管是否启用HITCHHIKE，都使用粗粒度收割：所有请求共享同一个op_id
    pending_reads_.push_back(PendingRead{fd, buf, len, offset, current_op_id});
    return current_op_id;
}

void IoRingWrapper::flush_batch() {
    if (pending_reads_.empty()) return;

    if (uring_unlikely(pending_reads_.size() >= 125)) { // sanity check
        printf("Too many pending reads in batch: %zu\n", pending_reads_.size());
        throw std::runtime_error("Too many pending reads in batch (>96), likely a bug");
    }

    // 粗粒度收割：无论是否启用HITCHHIKE，都使用共享的op_id
    uint64_t batch_op_id = next_op_id++;

    if (hitchhike_enabled()) {
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
        sqe->user_data = batch_op_id;  // 使用共享的batch_op_id

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
    } else {
        // 非HITCHHIKE路径：也使用粗粒度收割（所有请求共享同一个op_id）
        for (const auto& req : pending_reads_) {
            struct io_uring_sqe* sqe = get_sqe();
            if (!sqe) throw std::runtime_error("Failed to get SQE for read");
            
            io_uring_prep_read(sqe, req.fd, req.buf, req.len, req.offset);
            sqe->user_data = batch_op_id;  // 所有SQE使用相同的batch_op_id
        }
    }
    submit();
    pending_reads_.clear();
}

size_t IoRingWrapper::pending_requests_count() const {
    return pending_reads_.size();
}

void IoRingWrapper::clear_batch() {
    pending_reads_.clear();
}

std::vector<AsyncCompletion> IoRingWrapper::poll_completions(int max_events) {
    std::vector<AsyncCompletion> out;
    out.reserve(max_events);
    struct io_uring_cqe* cqe = nullptr;
    int got = 0;
    while (got < max_events) {
        int ret = peek_cqe(&cqe);
        if (ret != 0 || cqe == nullptr) {
            break;
        }
        AsyncCompletion ac;
        ac.op_id = cqe->user_data;
        ac.result = cqe->res;
        out.push_back(ac);
        cqe_seen(cqe); // 标记为已处理
        cqe = nullptr; // reset
        got++;
    }
    return out;
}
