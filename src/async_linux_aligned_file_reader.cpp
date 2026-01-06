// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <cassert>
#include <cstdio>
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include "utils.h"

#include "async_linux_aligned_file_reader.h"
#include "coroutine_scheduler.h"

AsyncLinuxAlignedFileReader::AsyncLinuxAlignedFileReader() 
    : file_desc(-1) {
}

AsyncLinuxAlignedFileReader::~AsyncLinuxAlignedFileReader() {
    // Check if file is still open and close it
    int64_t ret = ::fcntl(this->file_desc, F_GETFD);
    if (ret != -1) {
        ::close(this->file_desc);
    }
}

IOContext &AsyncLinuxAlignedFileReader::get_ctx() {
    // For compatibility with legacy code
    return dummy_ctx;
}

void AsyncLinuxAlignedFileReader::register_thread() {
    // In coroutine version, thread management is handled by scheduler
    // This is kept for API compatibility
}

void AsyncLinuxAlignedFileReader::deregister_thread() {
    // In coroutine version, thread management is handled by scheduler
    // This is kept for API compatibility
}

void AsyncLinuxAlignedFileReader::deregister_all_threads() {
    // In coroutine version, thread management is handled by scheduler
    // This is kept for API compatibility
}

void AsyncLinuxAlignedFileReader::open(const std::string &fname) {
    int flags = O_DIRECT | O_RDONLY | O_LARGEFILE;
    this->file_desc = ::open(fname.c_str(), flags);
    
    if (this->file_desc == -1) {
        throw std::runtime_error("Failed to open file: " + fname + ", error: " + strerror(errno));
    }
    
    // Get file size
    struct stat file_stat;
    if (fstat(this->file_desc, &file_stat) == -1) {
        ::close(this->file_desc);
        this->file_desc = -1;
        throw std::runtime_error("Failed to get file stats for: " + fname);
    }
    this->file_sz = file_stat.st_size;
    
    diskann::cout << "Opened async file: " << fname << ", size: " << file_sz << std::endl;
}

void AsyncLinuxAlignedFileReader::close() {
    if (this->file_desc != -1) {
        ::close(this->file_desc);
        this->file_desc = -1;
    }
}

void AsyncLinuxAlignedFileReader::read(std::vector<AlignedRead> &read_reqs, IOContext &ctx, bool async) {
    if (async || read_reqs.empty()) {
        // For backward compatibility, fall back to synchronous reads
        diskann::cout << "Async flag not supported in AsyncLinuxAlignedFileReader::read. Use async_read_coro instead." << std::endl;
    }
    
    // Validate alignment
    for (const auto &req : read_reqs) {
        if (!IS_ALIGNED(req.len, 512) || !IS_ALIGNED(req.offset, 512) || !IS_ALIGNED(req.buf, 512)) {
            throw std::runtime_error("Read request not properly aligned");
        }
    }
    
    // Perform synchronous reads for compatibility
    for (auto &req : read_reqs) {
        ssize_t bytes_read = pread(file_desc, req.buf, req.len, req.offset);
        if (bytes_read == -1) {
            throw std::runtime_error("pread failed: " + std::string(strerror(errno)));
        }
        if (static_cast<size_t>(bytes_read) != req.len) {
            throw std::runtime_error("Incomplete read: expected " + std::to_string(req.len) + 
                                   ", got " + std::to_string(bytes_read));
        }
    }
}

diskann::Task<void> AsyncLinuxAlignedFileReader::async_read_coro(std::vector<AlignedRead> &read_reqs) {
    diskann::CoroutineScheduler *scheduler = diskann::get_cor_scheduler();
    
    if (scheduler == nullptr) {
        throw std::runtime_error("Coroutine scheduler not initialized. Call init_scheduler() first.");
    }

    assert(this->file_desc != -1);
    
    // Validate alignment
    for (const auto &req : read_reqs) {
        if (!IS_ALIGNED(req.len, 512) || !IS_ALIGNED(req.offset, 512) || !IS_ALIGNED(req.buf, 512)) {
            throw std::runtime_error("Read request not properly aligned");
        }
    }
    
    // Submit all reads asynchronously
    std::vector<diskann::IOAwaitable> awaitables = co_await scheduler->async_read_batch(file_desc, read_reqs);
    
    // Wait for all reads to complete
    for (auto &awaitable : awaitables) {
        int result = co_await awaitable;
        if (result < 0) {
            throw std::runtime_error("Async read failed with error: " + std::to_string(-result));
        }
    }
    
    co_return;
}

diskann::Task<std::vector<int>> AsyncLinuxAlignedFileReader::async_read_batch(std::vector<AlignedRead> &read_reqs) {
    diskann::CoroutineScheduler *scheduler = diskann::get_cor_scheduler();
    if (scheduler == nullptr) {
        throw std::runtime_error("Coroutine scheduler not initialized. Call init_scheduler() first.");
    }

    assert(this->file_desc != -1);
    
    // Validate alignment
    for (const auto &req : read_reqs) {
        if (!IS_ALIGNED(req.len, 512) || !IS_ALIGNED(req.offset, 512) || !IS_ALIGNED(req.buf, 512)) {
            throw std::runtime_error("Read request not properly aligned");
        }
    }
    
    // Submit all reads asynchronously
    std::vector<diskann::IOAwaitable> awaitables = co_await scheduler->async_read_batch(file_desc, read_reqs);
    
    std::vector<int> results;
    results.reserve(awaitables.size());
    
    // Wait for all reads to complete and collect results
    for (auto &awaitable : awaitables) {
        int result = co_await awaitable;
        results.push_back(result);
    }
    
    co_return results;
}
