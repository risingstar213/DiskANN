#include "libaio_wrapper.h"
#include <cstring>
#include "async_io.h"
#include <vector>
#include <liburing.h>

#define __NR_io_submit_hit 452

static int io_submit_hit(io_context_t ctx, long nr, struct iocb **iocbpp, 
                                    struct hitchhiker **hit_bufs) {
    return syscall(__NR_io_submit_hit, ctx, nr, iocbpp, hit_bufs);
}

LibAioWrapper::LibAioWrapper(unsigned entries) {
    memset(&ctx_, 0, sizeof(ctx_));
    if (io_setup(entries, &ctx_) < 0) {
        throw std::runtime_error("io_setup failed");
    }
    entries_ = entries;
    iocb_bufs_ = std::make_unique<struct iocb[]>(entries);
    memset(&iocb_bufs_[0], 0, sizeof(struct iocb) * entries);
#ifdef ENABLE_HITCHHIKE
    hitchhike_bufs_ = std::make_unique<struct hitchhiker[]>(entries);
#endif
}

LibAioWrapper::~LibAioWrapper() {
    io_destroy(ctx_);
}

uint64_t LibAioWrapper::add_read_request(int fd, void* buf, size_t len, off_t offset) {
    uint64_t current_op_id = next_op_id_;
    // 不管是否启用HITCHHIKE，都使用粗粒度收割：所有请求共享同一个op_id
    pending_reads_.push_back(PendingRead{fd, buf, len, offset});
    return current_op_id;
}

void LibAioWrapper::flush_batch() {
    if (pending_reads_.empty()) return;
    if (uring_unlikely(pending_reads_.size() >= 125)) { // sanity check
        printf("Too many pending reads in batch: %zu\n", pending_reads_.size());
        throw std::runtime_error("Too many pending reads in batch (>96), likely a bug");
    }

#ifdef ENABLE_HITCHHIKE
    struct iocb* iocb_ptr = &iocb_bufs_[next_op_id_];
    io_prep_pread(iocb_ptr, pending_reads_[0].fd, pending_reads_[0].buf, pending_reads_[0].len, pending_reads_[0].offset);
    struct hitchhiker* hit = &hitchhike_bufs_[next_op_id_];
    hit->in_use = 0;

    if (pending_reads_.size() > 1) {
        for (size_t i = 1; i < pending_reads_.size(); ++i) {
            hit->addr[i-1] = (uint64_t)pending_reads_[i].offset;
            hit->iov[i-1] = (uint64_t)pending_reads_[i].buf;
        }
        hit->max = pending_reads_.size() - 2;
        hit->in_use = 1;
        hit->iov_use = 1;
        hit->size = 4096; // page size

        iocb_ptr->u.c.__pad3 = 4096; // HITCHHIKE标志
    }

    iocb_ptr->data = (void*)next_op_id_; // 所有请求使用相同的op_id
    io_submit_hit(ctx_, 1, &iocb_ptr, &hit);
#else
    std::vector<struct iocb*> iocb_ptrs;
    iocb_ptrs.reserve(pending_reads_.size());
    for (int32_t i = 0; i < pending_reads_.size(); ++i) {
        auto& req = pending_reads_[i];
        io_prep_pread(&iocb_bufs_[i], req.fd, req.buf, req.len, req.offset);
        iocb_bufs_[i].data = (void*)next_op_id_; // 所有请求使用相同的op_id
        iocb_ptrs.push_back(&iocb_bufs_[i]);
    }
    int ret = io_submit(ctx_, iocb_ptrs.size(), iocb_ptrs.data());
    if (ret < 0) {
        throw std::runtime_error("io_submit failed");
    }
#endif
    pending_reads_.clear();
    next_op_id_ = (next_op_id_ + 1) % entries_; // 提交后递增op_id，为下一个批次做准备
}

size_t LibAioWrapper::pending_requests_count() const {
    return pending_reads_.size();
}

void LibAioWrapper::clear_batch() {
    pending_reads_.clear();
}

std::vector<AsyncCompletion> LibAioWrapper::poll_completions(int max_events) {
    std::vector<AsyncCompletion> out;
    out.reserve(max_events);
    std::vector<struct io_event> events(max_events);

    struct timespec ts = {0,0};
    int got = io_getevents(ctx_, 1, max_events, events.data(), &ts);
    if (got < 0) {
        throw std::runtime_error("io_getevents failed");
    }
    for (int i = 0; i < got; ++i) {
        AsyncCompletion ac;
        // data was set to (void*)op_id in flush
        ac.op_id = (uint64_t)events[i].data;
        ac.result = events[i].res;
        out.push_back(ac);
    }

    return out;
}
