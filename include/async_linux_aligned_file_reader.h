// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once
#ifndef _WINDOWS

#include "aligned_file_reader.h"
#include "coroutine_scheduler.h"
#include <memory>

class AsyncLinuxAlignedFileReader : public AlignedFileReader
{
  private:
    uint64_t file_sz;
    FileHandle file_desc;
    diskann::CoroutineScheduler* scheduler;

  public:
    AsyncLinuxAlignedFileReader();
    ~AsyncLinuxAlignedFileReader();

    IOContext &get_ctx() override;
    
    // register thread-id for a context
    void register_thread() override;
    
    // de-register thread-id for a context
    void deregister_thread() override;
    void deregister_all_threads() override;

    // Open & close ops
    void open(const std::string &fname) override;
    void close() override;

    // Synchronous read for compatibility
    void read(std::vector<AlignedRead> &read_reqs, IOContext &ctx, bool async = false) override;
    
    // Async coroutine-based read
    diskann::Task<void> async_read_coro(std::vector<AlignedRead> &read_reqs);
    
    // Batch async read that returns when all reads complete
    diskann::Task<std::vector<int>> async_read_batch(std::vector<AlignedRead> &read_reqs);

private:
    // Compatibility IOContext for legacy API
    IOContext dummy_ctx;
};

#endif
