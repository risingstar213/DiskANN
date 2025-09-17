// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "async_pq_flash_index.h"
#include "timer.h"
#include "percentile_stats.h"
#include "pq.h"
#include "pq_scratch.h"
#include <chrono>

namespace diskann {

template <typename T, typename LabelT>
AsyncPQFlashIndex<T, LabelT>::AsyncPQFlashIndex(std::shared_ptr<AlignedFileReader> &fileReader,
                                                diskann::Metric metric)
    : PQFlashIndex<T, LabelT>(fileReader, metric) {
    // Try to cast to async reader
    async_reader = std::dynamic_pointer_cast<AsyncLinuxAlignedFileReader>(fileReader);
}

template <typename T, typename LabelT>
Task<void> AsyncPQFlashIndex<T, LabelT>::async_cached_beam_search(
    const T *query1, const uint64_t k_search, const uint64_t l_search,
    uint64_t *indices, float *distances, const uint64_t beam_width,
    const bool use_filter, const LabelT &filter_label,
    const uint32_t io_limit, const bool use_reorder_data, QueryStats *stats) {
    
    co_await async_search_impl(query1, k_search, l_search, indices, distances, 
                              beam_width, use_filter, filter_label, io_limit, 
                              use_reorder_data, stats);
}

template <typename T, typename LabelT>
Task<void> AsyncPQFlashIndex<T, LabelT>::async_batch_search(
    const T *queries, const uint64_t num_queries, const uint64_t query_aligned_dim,
    const uint64_t k_search, const uint64_t l_search,
    std::vector<std::vector<uint64_t>> &all_indices,
    std::vector<std::vector<float>> &all_distances,
    const uint64_t beam_width, const bool use_filter,
    const std::vector<LabelT> &filter_labels, const uint32_t io_limit,
    const bool use_reorder_data, std::vector<QueryStats> *all_stats) {
    
    // Resize output vectors
    all_indices.resize(num_queries);
    all_distances.resize(num_queries);
    
    // Create tasks for all queries
    std::vector<Task<void>> search_tasks;
    search_tasks.reserve(num_queries);
    
    for (uint64_t i = 0; i < num_queries; i++) {
        all_indices[i].resize(k_search);
        all_distances[i].resize(k_search);
        
        LabelT current_filter = static_cast<LabelT>(0);
        if (use_filter) {
            if (filter_labels.size() == 1) {
                current_filter = filter_labels[0];
            } else if (filter_labels.size() == num_queries) {
                current_filter = filter_labels[i];
            }
        }
        
        QueryStats *current_stats = all_stats ? &(*all_stats)[i] : nullptr;
        
        // Create search task for this query
        search_tasks.push_back(async_search_impl(
            queries + (i * query_aligned_dim), k_search, l_search,
            all_indices[i].data(), all_distances[i].data(),
            beam_width, use_filter, current_filter, io_limit,
            use_reorder_data, current_stats));
    }
    
    // Wait for all search tasks to complete
    for (auto &task : search_tasks) {
        co_await task;
    }
}

template <typename T, typename LabelT>
Task<void> AsyncPQFlashIndex<T, LabelT>::async_search_impl(
    const T *query1, const uint64_t k_search, const uint64_t l_search,
    uint64_t *indices, float *distances, const uint64_t beam_width,
    const bool use_filter, const LabelT &filter_label,
    const uint32_t io_limit, const bool use_reorder_data,
    QueryStats *stats) {
    
    // Basic validation
    uint64_t num_sector_per_nodes = DIV_ROUND_UP(this->_max_node_len, defaults::SECTOR_LEN);
    if (beam_width > num_sector_per_nodes * defaults::MAX_N_SECTOR_READS) {
        throw ANNException("Beamwidth too high for sector reads", -1, __FUNCSIG__, __FILE__, __LINE__);
    }
    
    // Get thread data (this needs to be thread-safe in coroutine context)
    ScratchStoreManager<SSDThreadData<T>> manager(this->_thread_data);
    auto data = manager.scratch_space();
    auto query_scratch = &(data->scratch);
    auto pq_query_scratch = query_scratch->pq_scratch();
    
    // Reset query scratch
    query_scratch->reset();
    
    // Initialize query data (similar to original cached_beam_search)
    float query_norm = 0;
    T *aligned_query_T = query_scratch->aligned_query_T();
    float *query_float = pq_query_scratch->aligned_query_float;
    float *query_rotated = pq_query_scratch->rotated_query;
    
    // Normalization (copy from original implementation)
    if (this->metric == diskann::Metric::INNER_PRODUCT || this->metric == diskann::Metric::COSINE) {
        uint64_t inherent_dim = (this->metric == diskann::Metric::COSINE) ? 
            this->_data_dim : (uint64_t)(this->_data_dim - 1);
        
        for (size_t i = 0; i < inherent_dim; i++) {
            aligned_query_T[i] = query1[i];
            query_norm += query1[i] * query1[i];
        }
        if (this->metric == diskann::Metric::INNER_PRODUCT) {
            aligned_query_T[this->_data_dim - 1] = 0;
        }
        
        query_norm = std::sqrt(query_norm);
        for (size_t i = 0; i < inherent_dim; i++) {
            aligned_query_T[i] = (T)(aligned_query_T[i] / query_norm);
        }
        pq_query_scratch->initialize(this->_data_dim, aligned_query_T);
    } else {
        for (size_t i = 0; i < this->_data_dim; i++) {
            aligned_query_T[i] = query1[i];
        }
        pq_query_scratch->initialize(this->_data_dim, aligned_query_T);
    }
    
    // Initialize buffers and structures
    T *data_buf = query_scratch->coord_scratch;
    _mm_prefetch((char *)data_buf, _MM_HINT_T1);
    
    char *sector_scratch = query_scratch->sector_scratch;
    uint64_t &sector_scratch_idx = query_scratch->sector_idx;
    const uint64_t num_sectors_per_node = this->_nnodes_per_sector > 0 ? 
        1 : DIV_ROUND_UP(this->_max_node_len, defaults::SECTOR_LEN);
    
    // PQ preprocessing
    this->_pq_table.preprocess_query(query_rotated);
    float *pq_dists = pq_query_scratch->aligned_pqtable_dist_scratch;
    this->_pq_table.populate_chunk_distances(query_rotated, pq_dists);
    
    float *dist_scratch = pq_query_scratch->aligned_dist_scratch;
    uint8_t *pq_coord_scratch = pq_query_scratch->aligned_pq_coord_scratch;
    
    // Lambda for distance computation
    auto compute_dists = [this, pq_coord_scratch, pq_dists](const uint32_t *ids, const uint64_t n_ids, float *dists_out) {
        diskann::aggregate_coords(ids, n_ids, this->data, this->_n_chunks, pq_coord_scratch);
        diskann::pq_dist_lookup(pq_coord_scratch, n_ids, this->_n_chunks, pq_dists, dists_out);
    };
    
    Timer query_timer, io_timer, cpu_timer;
    
    tsl::robin_set<uint64_t> &visited = query_scratch->visited;
    NeighborPriorityQueue &retset = query_scratch->retset;
    retset.reserve(l_search);
    std::vector<Neighbor> &full_retset = query_scratch->full_retset;
    
    // Find best medoid (copied from original)
    uint32_t best_medoid = 0;
    float best_dist = (std::numeric_limits<float>::max)();
    if (!use_filter) {
        for (uint64_t cur_m = 0; cur_m < this->_num_medoids; cur_m++) {
            float cur_expanded_dist = this->_dist_cmp_float->compare(
                query_float, this->_centroid_data + this->_aligned_dim * cur_m, 
                (uint32_t)this->_aligned_dim);
            if (cur_expanded_dist < best_dist) {
                best_medoid = this->_medoids[cur_m];
                best_dist = cur_expanded_dist;
            }
        }
    } else {
        if (this->_filter_to_medoid_ids.find(filter_label) != this->_filter_to_medoid_ids.end()) {
            const auto &medoid_ids = this->_filter_to_medoid_ids.at(filter_label);
            for (uint64_t cur_m = 0; cur_m < medoid_ids.size(); cur_m++) {
                compute_dists(&medoid_ids[cur_m], 1, dist_scratch);
                float cur_expanded_dist = dist_scratch[0];
                if (cur_expanded_dist < best_dist) {
                    best_medoid = medoid_ids[cur_m];
                    best_dist = cur_expanded_dist;
                }
            }
        } else {
            throw ANNException("Cannot find medoid for specified filter.", -1, __FUNCSIG__, __FILE__, __LINE__);
        }
    }
    
    compute_dists(&best_medoid, 1, dist_scratch);
    retset.insert(Neighbor(best_medoid, dist_scratch[0]));
    visited.insert(best_medoid);
    
    uint32_t num_ios = 0;
    
    // Main search loop with async IO
    std::vector<uint32_t> frontier;
    frontier.reserve(2 * beam_width);
    std::vector<std::pair<uint32_t, char *>> frontier_nhoods;
    frontier_nhoods.reserve(2 * beam_width);
    std::vector<AlignedRead> frontier_read_reqs;
    frontier_read_reqs.reserve(2 * beam_width);
    std::vector<std::pair<uint32_t, std::pair<uint32_t, uint32_t *>>> cached_nhoods;
    cached_nhoods.reserve(2 * beam_width);
    
    while (retset.has_unexpanded_node() && num_ios < io_limit) {
        // Clear iteration state
        frontier.clear();
        frontier_nhoods.clear();
        frontier_read_reqs.clear();
        cached_nhoods.clear();
        sector_scratch_idx = 0;
        
        // Find new beam
        uint32_t num_seen = 0;
        while (retset.has_unexpanded_node() && frontier.size() < beam_width && num_seen < beam_width) {
            auto nbr = retset.closest_unexpanded();
            num_seen++;
            
            auto iter = this->_nhood_cache.find(nbr.id);
            if (iter != this->_nhood_cache.end()) {
                cached_nhoods.push_back(std::make_pair(nbr.id, iter->second));
                if (stats != nullptr) {
                    stats->n_cache_hits++;
                }
            } else {
                frontier.push_back(nbr.id);
            }
            
            if (this->_count_visited_nodes) {
                reinterpret_cast<std::atomic<uint32_t> &>(this->_node_visit_counter[nbr.id].second).fetch_add(1);
            }
        }
        
        // Prepare async reads for frontier
        if (!frontier.empty()) {
            if (stats != nullptr) stats->n_hops++;
            
            for (uint64_t i = 0; i < frontier.size(); i++) {
                auto id = frontier[i];
                std::pair<uint32_t, char *> fnhood;
                fnhood.first = id;
                fnhood.second = sector_scratch + num_sectors_per_node * sector_scratch_idx * defaults::SECTOR_LEN;
                sector_scratch_idx++;
                frontier_nhoods.push_back(fnhood);
                frontier_read_reqs.emplace_back(
                    this->get_node_sector((size_t)id) * defaults::SECTOR_LEN,
                    num_sectors_per_node * defaults::SECTOR_LEN, 
                    fnhood.second);
                
                if (stats != nullptr) {
                    stats->n_4k++;
                    stats->n_ios++;
                }
                num_ios++;
            }
            
            // Perform async reads
            io_timer.reset();
            co_await async_read_frontier_nhoods(frontier_nhoods, frontier_read_reqs, stats);
            if (stats != nullptr) {
                stats->io_us += (float)io_timer.elapsed();
            }
        }
        
        // Process cached neighborhoods (this part remains synchronous)
        for (auto &cached_nhood : cached_nhoods) {
            auto global_cache_iter = this->_coord_cache.find(cached_nhood.first);
            T *node_fp_coords_copy = global_cache_iter->second;
            float cur_expanded_dist;
            
            if (!this->_use_disk_index_pq) {
                cur_expanded_dist = this->_dist_cmp->compare(aligned_query_T, node_fp_coords_copy, (uint32_t)this->_aligned_dim);
            } else {
                if (this->metric == diskann::Metric::INNER_PRODUCT) {
                    cur_expanded_dist = this->_disk_pq_table.inner_product(query_float, (uint8_t *)node_fp_coords_copy);
                } else {
                    cur_expanded_dist = this->_disk_pq_table.l2_distance(query_float, (uint8_t *)node_fp_coords_copy);
                }
            }
            full_retset.push_back(Neighbor((uint32_t)cached_nhood.first, cur_expanded_dist));
            
            uint64_t nnbrs = cached_nhood.second.first;
            uint32_t *node_nbrs = cached_nhood.second.second;
            
            cpu_timer.reset();
            compute_dists(node_nbrs, nnbrs, dist_scratch);
            if (stats != nullptr) {
                stats->n_cmps += (uint32_t)nnbrs;
                stats->cpu_us += (float)cpu_timer.elapsed();
            }
            
            for (uint64_t m = 0; m < nnbrs; ++m) {
                uint32_t id = node_nbrs[m];
                if (visited.insert(id).second) {
                    if (!use_filter && this->_dummy_pts.find(id) != this->_dummy_pts.end())
                        continue;
                    
                    if (use_filter && !(this->point_has_label(id, filter_label)) &&
                        (!this->_use_universal_label || !this->point_has_label(id, this->_universal_filter_label)))
                        continue;
                    
                    float dist = dist_scratch[m];
                    Neighbor nn(id, dist);
                    retset.insert(nn);
                }
            }
        }
        
        // Process frontier neighborhoods
        for (auto &frontier_nhood : frontier_nhoods) {
            char *node_disk_buf = this->offset_to_node(frontier_nhood.second, frontier_nhood.first);
            uint32_t *node_buf = this->offset_to_node_nhood(node_disk_buf);
            uint64_t nnbrs = (uint64_t)(*node_buf);
            T *node_fp_coords = this->offset_to_node_coords(node_disk_buf);
            memcpy(data_buf, node_fp_coords, this->_disk_bytes_per_point);
            
            float cur_expanded_dist;
            if (!this->_use_disk_index_pq) {
                cur_expanded_dist = this->_dist_cmp->compare(aligned_query_T, data_buf, (uint32_t)this->_aligned_dim);
            } else {
                if (this->metric == diskann::Metric::INNER_PRODUCT) {
                    cur_expanded_dist = this->_disk_pq_table.inner_product(query_float, (uint8_t *)data_buf);
                } else {
                    cur_expanded_dist = this->_disk_pq_table.l2_distance(query_float, (uint8_t *)data_buf);
                }
            }
            full_retset.push_back(Neighbor(frontier_nhood.first, cur_expanded_dist));
            
            uint32_t *node_nbrs = node_buf + 1;
            
            cpu_timer.reset();
            compute_dists(node_nbrs, nnbrs, dist_scratch);
            if (stats != nullptr) {
                stats->n_cmps += (uint32_t)nnbrs;
                stats->cpu_us += (float)cpu_timer.elapsed();
            }
            
            for (uint64_t m = 0; m < nnbrs; ++m) {
                uint32_t id = node_nbrs[m];
                if (visited.insert(id).second) {
                    if (!use_filter && this->_dummy_pts.find(id) != this->_dummy_pts.end())
                        continue;
                    
                    if (use_filter && !(this->point_has_label(id, filter_label)) &&
                        (!this->_use_universal_label || !this->point_has_label(id, this->_universal_filter_label)))
                        continue;
                    
                    float dist = dist_scratch[m];
                    Neighbor nn(id, dist);
                    retset.insert(nn);
                }
            }
        }
    }
    
    // Extract top-k results (same as original)
    size_t pos = 0;
    for (size_t i = 0; i < l_search && i < retset.size() && pos < k_search; i++) {
        if (!use_filter && this->_dummy_pts.find(retset[i].id) != this->_dummy_pts.end()) {
            continue;
        }
        if (use_filter && !(this->point_has_label(retset[i].id, filter_label)) &&
            (!this->_use_universal_label || !this->point_has_label(retset[i].id, this->_universal_filter_label))) {
            continue;
        }
        indices[pos] = retset[i].id;
        distances[pos] = retset[i].distance;
        pos++;
    }
    
    // Handle reordering if needed
    if (use_reorder_data && pos > 0) {
        // Reordering logic (simplified - would need full implementation)
        diskann::cout << "Reordering not yet implemented in async version" << std::endl;
    }
    
    co_return;
}

template <typename T, typename LabelT>
Task<void> AsyncPQFlashIndex<T, LabelT>::async_read_frontier_nhoods(
    std::vector<std::pair<uint32_t, char *>> &frontier_nhoods,
    std::vector<AlignedRead> &frontier_read_reqs,
    QueryStats *stats) {
    
    if (!async_reader) {
        throw std::runtime_error("Async reader not available");
    }
    
    // Use the async reader to perform all reads concurrently
    co_await async_reader->async_read_coro(frontier_read_reqs);
}

// Explicit template instantiations
template class AsyncPQFlashIndex<float>;
template class AsyncPQFlashIndex<int8_t>;
template class AsyncPQFlashIndex<uint8_t>;

template class AsyncPQFlashIndex<float, uint16_t>;
template class AsyncPQFlashIndex<int8_t, uint16_t>;
template class AsyncPQFlashIndex<uint8_t, uint16_t>;

} // namespace diskann
