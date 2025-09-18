#include "io_ring_wrapper.h"
#include "aligned_file_reader.h" // for AlignedRead

IoRingWrapper::IoRingWrapper(unsigned entries, unsigned flags) {
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

void IoRingWrapper::add_read_request(int fd, void* buf, size_t len, off_t offset, uint64_t op_id) {
    pending_reads_.push_back(PendingRead{fd, buf, len, offset, op_id});
}

void IoRingWrapper::flush_batch() {
    if (pending_reads_.empty()) return;
    
    for (const auto& req : pending_reads_) {
        struct io_uring_sqe* sqe = get_sqe();
        if (!sqe) throw std::runtime_error("Failed to get SQE for read");
        
        io_uring_prep_read(sqe, req.fd, req.buf, req.len, req.offset);
        sqe->user_data = req.op_id;
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
