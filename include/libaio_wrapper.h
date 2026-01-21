// libaio_wrapper.h
// 封装libaio相关API，接口风格仿照io_ring_wrapper
#pragma once
#include <libaio.h>
#include <liburing.h>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <sys/types.h>
#include <vector>
#include "async_io.h"

class LibAioWrapper : public AsyncIO {
public:
    LibAioWrapper(unsigned entries);
    ~LibAioWrapper();

    // 提交一个read请求
    uint64_t add_read_request(int fd, void* buf, size_t len, off_t offset) override;
    // 批量提交所有pending的read请求
    void flush_batch() override;
    // Poll unified completions
    std::vector<AsyncCompletion> poll_completions(int max_events) override;
    // 当前pending请求数
    size_t pending_requests_count() const override;
    // 清空batch
    void clear_batch() override;

private:
    io_context_t ctx_;
    uint64_t entries_;
    uint64_t next_op_id_ = 1;
    struct PendingRead {
        int fd;
        void* buf;
        size_t len;
        off_t offset;
    };
    std::vector<PendingRead> pending_reads_;

    std::unique_ptr<struct iocb[]> iocb_bufs_;
    std::unique_ptr<struct hitchhiker[]> hitchhike_bufs_;
};
