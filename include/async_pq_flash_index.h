// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "pq_flash_index.h"
#include "coroutine_scheduler.h"
#include "async_linux_aligned_file_reader.h"

namespace diskann {

// Forward declaration for coroutine-aware version
template <typename T, typename LabelT = uint32_t> 
class AsyncPQFlashIndex : public PQFlashIndex<T, LabelT> {
public:
    DISKANN_DLLEXPORT AsyncPQFlashIndex(std::shared_ptr<AlignedFileReader> &fileReader,
                                        diskann::Metric metric = diskann::Metric::L2);

    // Async coroutine version of cached_beam_search
    DISKANN_DLLEXPORT Task<void> async_cached_beam_search(
        const T *query1, const uint64_t k_search, const uint64_t l_search,
        uint64_t *indices, float *distances, const uint64_t beam_width,
        const bool use_filter = false, const LabelT &filter_label = static_cast<LabelT>(0),
        const uint32_t io_limit = std::numeric_limits<uint32_t>::max(),
        const bool use_reorder_data = false, QueryStats *stats = nullptr);

    // Batch async search for multiple queries
    DISKANN_DLLEXPORT Task<void> async_batch_search(
        const T *queries, const uint64_t num_queries, const uint64_t query_aligned_dim,
        const uint64_t k_search, const uint64_t l_search,
        std::vector<std::vector<uint64_t>> &all_indices,
        std::vector<std::vector<float>> &all_distances,
        const uint64_t beam_width,
        const bool use_filter = false, 
        const std::vector<LabelT> &filter_labels = {},
        const uint32_t io_limit = std::numeric_limits<uint32_t>::max(),
        const bool use_reorder_data = false,
        std::vector<QueryStats> *all_stats = nullptr);

private:
    // Async version of the core search logic
    DISKANN_DLLEXPORT Task<void> async_search_impl(
        const T *query1, const uint64_t k_search, const uint64_t l_search,
        uint64_t *indices, float *distances, const uint64_t beam_width,
        const bool use_filter, const LabelT &filter_label,
        const uint32_t io_limit, const bool use_reorder_data,
        QueryStats *stats);

    // Helper to handle async IO operations
    DISKANN_DLLEXPORT Task<void> async_read_frontier_nhoods(
        std::vector<std::pair<uint32_t, char *>> &frontier_nhoods,
        std::vector<AlignedRead> &frontier_read_reqs,
        QueryStats *stats);

    std::shared_ptr<AsyncLinuxAlignedFileReader> async_reader;
};

// Utility to create async version
template<typename T, typename LabelT = uint32_t>
std::unique_ptr<AsyncPQFlashIndex<T, LabelT>> create_async_pq_flash_index(
    diskann::Metric metric = diskann::Metric::L2) {
    
    std::shared_ptr<AlignedFileReader> reader(new AsyncLinuxAlignedFileReader());
    return std::make_unique<AsyncPQFlashIndex<T, LabelT>>(reader, metric);
}

} // namespace diskann
