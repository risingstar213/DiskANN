// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "async_pq_flash_index.h"
#include "timer.h"
#include "percentile_stats.h"
#include "pq.h"
#include "pq_scratch.h"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <iostream>
#include <set>

const int hrs[HRS_LEN] = HRS_VALUE;
const double sp[SAMPLE_POINT_NUM][2] = SAMPLE_POINT_VALUE;

#define register_hitnum_reset_nn(n, hitnum_reset_nn, recall_reset_nn_split, time_reset_n, io_reset_n,                 \
                                 hitnum_reset_n_final, len_frf_n)                                                     \
    if (step_num == n)                                                                                                \
    {                                                                                                                 \
        if (STEP_TIME)                                                                                                \
        {                                                                                                             \
            hitnum_reset_nn = 1;                                                                                      \
            for (int cnt = 0; cnt < SAMPLE_POINT_NUM; cnt++)                                                          \
                recall_reset_nn_split[cnt] = 1;                                                                       \
            auto end_ts = std::chrono::system_clock::now();                                                           \
            auto last = std::chrono::duration_cast<std::chrono::microseconds>(end_ts - start);                        \
            time_reset_n = static_cast<unsigned>(last.count());                                                       \
        }                                                                                                             \
        else                                                                                                          \
        {                                                                                                             \
            io_reset_n = stats->n_ios;                                                                                \
            if (!full_retset.empty())                                                                                 \
            {                                                                                                         \
                res_ids.assign(full_retset.size(), 0);                                                                \
                for (size_t i = 0; i < full_retset.size(); i++)                                                       \
                {                                                                                                     \
                    res_ids[i] = full_retset[i].id;                                                                   \
                }                                                                                                     \
                res.clear();                                                                                          \
                res.insert(res_ids.begin(), res_ids.end());                                                           \
                hitnum_reset_nn = 0;                                                                                  \
                for (auto &v : gt)                                                                                    \
                {                                                                                                     \
                    if (res.find(v) != res.end())                                                                     \
                    {                                                                                                 \
                        hitnum_reset_nn++;                                                                            \
                    }                                                                                                 \
                }                                                                                                     \
                for (int cnt = 0; cnt < SAMPLE_POINT_NUM; cnt++)                                                      \
                {                                                                                                     \
                    res.clear();                                                                                      \
                    size_t start_idx = std::min(res_ids.size(),                                                       \
                                               static_cast<size_t>(cur_list_size * sp[cnt][0]));                      \
                    size_t end_idx = std::min(res_ids.size(),                                                         \
                                             static_cast<size_t>(cur_list_size * sp[cnt][1]));                        \
                    auto start_it = res_ids.begin() + start_idx;                                                      \
                    auto end_it = res_ids.begin() + end_idx;                                                          \
                    res.insert(start_it, end_it);                                                                     \
                    recall_reset_nn_split[cnt] = 0;                                                                   \
                    for (auto &v : gt)                                                                                \
                    {                                                                                                 \
                        if (res.find(v) != res.end())                                                                 \
                        {                                                                                             \
                            recall_reset_nn_split[cnt]++;                                                             \
                        }                                                                                             \
                    }                                                                                                 \
                }                                                                                                     \
            }                                                                                                         \
            len_frf_n = static_cast<unsigned>(full_retset_final_ids.size());                                          \
            if (!full_retset_final_ids.empty())                                                                       \
            {                                                                                                         \
                res_ids.assign(full_retset_final_ids.begin(), full_retset_final_ids.end());                           \
                res.clear();                                                                                          \
                res.insert(res_ids.begin(), res_ids.end());                                                           \
                hitnum_reset_n_final = 0;                                                                             \
                for (auto &v : gt)                                                                                    \
                {                                                                                                     \
                    if (res.find(v) != res.end())                                                                     \
                    {                                                                                                 \
                        hitnum_reset_n_final++;                                                                       \
                    }                                                                                                 \
                }                                                                                                     \
            }                                                                                                         \
        }                                                                                                             \
    }

#define register_hitnum_reset_nn_after(n, hitnum_reset_nn, recall_reset_nn_split, time_reset_n, io_reset_n,          \
                                       hitnum_reset_n_final, len_frf_n)                                               \
    if (step_num < n)                                                                                                \
    {                                                                                                                 \
        if (STEP_TIME)                                                                                                \
        {                                                                                                             \
            hitnum_reset_nn = 1;                                                                                      \
            for (int cnt = 0; cnt < SAMPLE_POINT_NUM; cnt++)                                                          \
                recall_reset_nn_split[cnt] = 1;                                                                       \
            auto end_ts = std::chrono::system_clock::now();                                                           \
            auto last = std::chrono::duration_cast<std::chrono::microseconds>(end_ts - start);                        \
            time_reset_n = static_cast<unsigned>(last.count());                                                       \
        }                                                                                                             \
        else                                                                                                          \
        {                                                                                                             \
            io_reset_n = stats->n_ios;                                                                                \
            res_ids.assign(retset.size(), 0);                                                                         \
            for (size_t i = 0; i < retset.size(); i++)                                                                \
            {                                                                                                         \
                res_ids[i] = retset[i].id;                                                                            \
            }                                                                                                         \
            res.clear();                                                                                              \
            res.insert(res_ids.begin(), res_ids.end());                                                               \
            hitnum_reset_nn = 0;                                                                                      \
            for (auto &v : gt)                                                                                        \
            {                                                                                                         \
                if (res.find(v) != res.end())                                                                         \
                {                                                                                                     \
                    hitnum_reset_nn++;                                                                                \
                }                                                                                                     \
            }                                                                                                         \
            if (!full_retset_final_ids.empty())                                                                       \
            {                                                                                                         \
                res_ids.assign(full_retset_final_ids.begin(), full_retset_final_ids.end());                           \
                res.clear();                                                                                          \
                res.insert(res_ids.begin(), res_ids.end());                                                           \
                hitnum_reset_n_final = 0;                                                                             \
                for (auto &v : gt)                                                                                    \
                {                                                                                                     \
                    if (res.find(v) != res.end())                                                                     \
                    {                                                                                                 \
                        hitnum_reset_n_final++;                                                                       \
                    }                                                                                                 \
                }                                                                                                     \
            }                                                                                                         \
            len_frf_n = static_cast<unsigned>(full_retset_final_ids.size());                                          \
            if (!full_retset_final_ids.empty())                                                                       \
            {                                                                                                         \
                for (int cnt = 0; cnt < SAMPLE_POINT_NUM; cnt++)                                                      \
                {                                                                                                     \
                    size_t start_idx =                                                                                \
                        std::min(full_retset_final_ids.size(),                                                        \
                                 static_cast<size_t>(cur_list_size * sp[cnt][0]));                                    \
                    size_t end_idx =                                                                                  \
                        std::min(full_retset_final_ids.size(),                                                        \
                                 static_cast<size_t>(cur_list_size * sp[cnt][1]));                                    \
                    auto start_it = full_retset_final_ids.begin() + start_idx;                                       \
                    auto end_it = full_retset_final_ids.begin() + end_idx;                                            \
                    res.clear();                                                                                      \
                    res.insert(start_it, end_it);                                                                     \
                    recall_reset_nn_split[cnt] = 0;                                                                   \
                    for (auto &v : gt)                                                                                \
                    {                                                                                                 \
                        if (res.find(v) != res.end())                                                                 \
                        {                                                                                             \
                            recall_reset_nn_split[cnt]++;                                                             \
                        }                                                                                             \
                    }                                                                                                 \
                }                                                                                                     \
            }                                                                                                         \
        }                                                                                                             \
    }

#ifdef PRINT_PATH
#define print_reset()                                                                                                  \
    if (print_flag)                                                                                                    \
    {                                                                                                                 \
        std::cout << "retset:";                                                                                      \
        for (size_t i = 0; i < retset.size(); i++)                                                                     \
        {                                                                                                             \
            std::cout << retset[i].id << " ";                                                                        \
        }                                                                                                             \
        std::cout << std::endl;                                                                                        \
    }

#define print_fullreset()                                                                                              \
    if (print_flag)                                                                                                    \
    {                                                                                                                 \
        std::cout << "full_rst:[size=" << full_retset.size() << "]:";                                              \
        for (auto &i : full_retset)                                                                                    \
        {                                                                                                             \
            std::cout << i.id << " ";                                                                                \
        }                                                                                                             \
        std::cout << std::endl << std::endl;                                                                           \
    }
#else
#define print_reset()
#define print_fullreset()
#endif

// Macros for reordering data access - same as in pq_flash_index.cpp
#ifndef VECTOR_SECTOR_NO
#define VECTOR_SECTOR_NO(id) (((uint64_t)(id)) / this->_nvecs_per_sector + this->_reorder_data_start_sector)
#endif
#ifndef VECTOR_SECTOR_OFFSET
#define VECTOR_SECTOR_OFFSET(id) ((((uint64_t)(id)) % this->_nvecs_per_sector) * this->_data_dim * sizeof(T))
#endif

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
    const uint32_t io_limit, const bool use_reorder_data, QueryStats *stats,
    const SearchStreamOptions *streaming) {
    
    co_await async_search_impl(query1, k_search, l_search, indices, distances, 
                              beam_width, use_filter, filter_label, io_limit, 
                              use_reorder_data, stats, streaming);

    co_return;
}

template <typename T, typename LabelT>
Task<void> AsyncPQFlashIndex<T, LabelT>::async_batch_search(
    const T *queries, const uint64_t num_queries, const uint64_t query_aligned_dim,
    const uint64_t k_search, const uint64_t l_search,
    std::vector<std::vector<uint64_t>> &all_indices,
    std::vector<std::vector<float>> &all_distances,
    const uint64_t beam_width, const bool use_filter,
    const std::vector<LabelT> &filter_labels, const uint32_t io_limit,
    const bool use_reorder_data, std::vector<QueryStats> *all_stats,
    const std::vector<SearchStreamOptions> *streaming_options) {
    
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
        const SearchStreamOptions *per_query_streaming = nullptr;
        if (streaming_options != nullptr && !streaming_options->empty()) {
            if (streaming_options->size() == 1) {
                per_query_streaming = &(*streaming_options)[0];
            } else if (streaming_options->size() == num_queries) {
                per_query_streaming = &(*streaming_options)[i];
            }
        }

        search_tasks.push_back(async_search_impl(
            queries + (i * query_aligned_dim), k_search, l_search,
            all_indices[i].data(), all_distances[i].data(),
            beam_width, use_filter, current_filter, io_limit,
            use_reorder_data, current_stats, per_query_streaming));
    }
    
    // Wait for all search tasks to complete
    for (auto &task : search_tasks) {
        co_await task;
    }

    co_return;
}

template <typename T, typename LabelT>
Task<void> AsyncPQFlashIndex<T, LabelT>::async_search_impl(
    const T *query1, const uint64_t k_search, const uint64_t l_search,
    uint64_t *indices, float *distances, const uint64_t beam_width,
    const bool use_filter, const LabelT &filter_label,
    const uint32_t io_limit, const bool use_reorder_data,
    QueryStats *stats, const SearchStreamOptions *streaming) {
    
    // Basic validation - same as original
    uint64_t num_sector_per_nodes = DIV_ROUND_UP(this->_max_node_len, defaults::SECTOR_LEN);
    if (beam_width > num_sector_per_nodes * defaults::MAX_N_SECTOR_READS) {
        throw ANNException("Beamwidth can not be higher than defaults::MAX_N_SECTOR_READS", -1, __FUNCSIG__, __FILE__, __LINE__);
    }
    
    // Get thread data - same as original
    ScratchStoreManager<SSDThreadData<T>> manager(this->_thread_data);
    auto data = manager.scratch_space();
    auto query_scratch = &(data->scratch);
    auto pq_query_scratch = query_scratch->pq_scratch();

    const bool streaming_enabled = (streaming != nullptr && streaming->emit != nullptr && streaming->stage_count > 0);
    const bool multi_stage_streaming = streaming_enabled && streaming->stage_count >= 2;
    uint32_t stage_one_target = 0;
    const uint32_t final_stage_idx = streaming_enabled ? (streaming->stage_count - 1) : 0;
    const uint32_t min_ios_before_emit = (streaming_enabled ? streaming->min_ios_before_emit : 0);
    const uint32_t min_steps_before_emit = (streaming_enabled ? streaming->min_steps_before_emit : 0);
    bool stage_one_emitted = false;
    size_t stage_emit_cursor = 0;
    std::vector<uint32_t> stage_ids_buffer;
    std::vector<float> stage_dist_buffer;
    tsl::robin_set<uint32_t> stage_emitted_ids;
    if (streaming_enabled) {
        uint64_t capped_k = std::min<uint64_t>(k_search, static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()));
        stage_ids_buffer.reserve(static_cast<size_t>(capped_k));
        if (streaming->include_distances) {
            stage_dist_buffer.reserve(stage_ids_buffer.capacity());
        }
        stage_emitted_ids.reserve(static_cast<size_t>(capped_k));

        if (multi_stage_streaming) {
            if (streaming->first_stage_min_results > 0) {
                stage_one_target = std::min<uint32_t>(static_cast<uint32_t>(k_search), streaming->first_stage_min_results);
            } else {
                float ratio = (streaming->first_stage_fraction <= 0.0f) ? 0.2f : streaming->first_stage_fraction;
                auto tentative = static_cast<uint32_t>(
                    std::ceil(ratio * static_cast<float>(std::min<uint64_t>(k_search, capped_k))));
                stage_one_target = std::min<uint32_t>(static_cast<uint32_t>(k_search), std::max<uint32_t>(tentative, 1u));
            }
        }
    }

    auto emit_stage = [&](uint32_t stage_idx, bool is_final, const std::vector<Neighbor> &source, size_t target_total) {
        if (!streaming_enabled || streaming->emit == nullptr) {
            return;
        }
        size_t capped_target = std::min(target_total, source.size());
        if (capped_target < stage_emit_cursor) {
            capped_target = stage_emit_cursor;
        }

        size_t existing_unique = stage_emitted_ids.size();
        if (capped_target < existing_unique) {
            capped_target = existing_unique;
        }

        size_t desired_unique = capped_target;
        size_t needed_new = desired_unique > existing_unique ? (desired_unique - existing_unique) : 0;

        const uint32_t *id_ptr = nullptr;
        const float *dist_ptr = nullptr;

        if (needed_new > 0) {
            stage_ids_buffer.clear();
            stage_ids_buffer.reserve(needed_new);
            if (streaming->include_distances) {
                stage_dist_buffer.clear();
                stage_dist_buffer.reserve(needed_new);
            }

            for (size_t source_idx = 0; source_idx < source.size() && stage_ids_buffer.size() < needed_new; ++source_idx) {
                uint32_t id = source[source_idx].id;
                if (this->_dummy_pts.find(id) != this->_dummy_pts.end()) {
                    id = this->_dummy_to_real_map[id];
                }

                auto insert_result = stage_emitted_ids.insert(id);
                if (!insert_result.second) {
                    continue;
                }

                stage_ids_buffer.push_back(id);
                if (streaming->include_distances) {
                    stage_dist_buffer.push_back(source[source_idx].distance);
                }
            }

            needed_new = stage_ids_buffer.size();
            if (needed_new > 0) {
                id_ptr = stage_ids_buffer.data();
                dist_ptr = streaming->include_distances ? stage_dist_buffer.data() : nullptr;
            }
        }

        if (needed_new == 0 && !is_final) {
            return;
        }

        stage_emit_cursor = stage_emitted_ids.size();

        streaming->emit(streaming->user_context, streaming->query_id, stage_idx, id_ptr, dist_ptr, needed_new,
                        is_final);
    };
    
    // Reset query scratch - same as original
    query_scratch->reset();
    
    // Copy query to thread specific aligned memory - same as original
    float query_norm = 0;
    T *aligned_query_T = query_scratch->aligned_query_T();
    float *query_float = pq_query_scratch->aligned_query_float;
    float *query_rotated = pq_query_scratch->rotated_query;
    
    // Normalization step - exactly same as original
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
    
    // Pointers to buffers for data - same as original
    T *data_buf = query_scratch->coord_scratch;
    _mm_prefetch((char *)data_buf, _MM_HINT_T1);
    
    // Sector scratch - same as original
    char *sector_scratch = query_scratch->sector_scratch;
    uint64_t &sector_scratch_idx = query_scratch->sector_idx;
    const uint64_t num_sectors_per_node = this->_nnodes_per_sector > 0 ? 
        1 : DIV_ROUND_UP(this->_max_node_len, defaults::SECTOR_LEN);
    
    // Query <-> PQ chunk centers distances - same as original
    this->_pq_table.preprocess_query(query_rotated);
    float *pq_dists = pq_query_scratch->aligned_pqtable_dist_scratch;
    this->_pq_table.populate_chunk_distances(query_rotated, pq_dists);
    
    // Query <-> neighbor list - same as original
    float *dist_scratch = pq_query_scratch->aligned_dist_scratch;
    uint8_t *pq_coord_scratch = pq_query_scratch->aligned_pq_coord_scratch;
    
    // Lambda to batch compute query<-> node distances in PQ space - same as original
    auto compute_dists = [this, pq_coord_scratch, pq_dists](const uint32_t *ids, const uint64_t n_ids, float *dists_out) {
        diskann::aggregate_coords(ids, n_ids, this->data, this->_n_chunks, pq_coord_scratch);
        diskann::pq_dist_lookup(pq_coord_scratch, n_ids, this->_n_chunks, pq_dists, dists_out);
    };
    
    Timer query_timer, io_timer, cpu_timer;
    auto start = std::chrono::system_clock::now();
    
    tsl::robin_set<uint64_t> &visited = query_scratch->visited;
    NeighborPriorityQueue &retset = query_scratch->retset;
    retset.reserve(l_search);
    std::vector<Neighbor> &full_retset = query_scratch->full_retset;
    std::vector<Neighbor> full_retset_final;
    full_retset_final.reserve(4096);
    std::vector<uint32_t> full_retset_final_ids;
    full_retset_final_ids.reserve(4096);
    std::vector<uint32_t> res_ids;
    res_ids.reserve(static_cast<size_t>(l_search + 1));

    bool gt_flag = false;
    std::set<unsigned> gt;
    std::set<unsigned> res;
    if (stats != nullptr && stats->gold_std != nullptr && stats->recall_at > 0 && stats->dim_gs > 0)
    {
        gt_flag = true;
        unsigned *gt_vec = stats->gold_std + static_cast<size_t>(stats->dim_gs) * stats->queries_id;
        size_t tie_breaker = stats->recall_at;
        if (stats->gs_dist != nullptr && stats->recall_at > 0)
        {
            tie_breaker = stats->recall_at - 1;
            float *gt_dist_vec = stats->gs_dist + static_cast<size_t>(stats->dim_gs) * stats->queries_id;
            while (tie_breaker < stats->dim_gs && gt_dist_vec[tie_breaker] == gt_dist_vec[stats->recall_at - 1])
            {
                tie_breaker++;
            }
        }
        tie_breaker = std::min<size_t>(tie_breaker, stats->dim_gs);
        gt.insert(gt_vec, gt_vec + tie_breaker);
    }

    unsigned step_num = 0;
    unsigned next_step_po = 1;
    unsigned updata_lastpos = 0;
    float award = 0.0f;
    bool prefetch_over = false;
    bool force_terminate = false;
    const bool use_cache = true;
    int cache_hitnum = 0;
    unsigned cur_list_size = 0;
    const bool print_flag = false;
    
    // Find best medoid - exactly same as original
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
                // For filtered index, use PQ distance as approximation - same as original
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
    
    uint32_t cmps = 0;
    uint32_t hops = 0;
    uint32_t num_ios = 0;
    
    // Cleared every iteration - same as original
    std::vector<uint32_t> frontier;
    frontier.reserve(2 * beam_width);
    std::vector<std::pair<uint32_t, char *>> frontier_nhoods;
    frontier_nhoods.reserve(2 * beam_width);
    std::vector<AlignedRead> frontier_read_reqs;
    frontier_read_reqs.reserve(2 * beam_width);
    std::vector<std::pair<uint32_t, std::pair<uint32_t, uint32_t *>>> cached_nhoods;
    cached_nhoods.reserve(2 * beam_width);
    
    while (retset.has_unexpanded_node() && num_ios < io_limit) {
        print_reset();
        print_fullreset();
        // Clear iteration state - same as original
        frontier.clear();
        frontier_nhoods.clear();
        frontier_read_reqs.clear();
        cached_nhoods.clear();
        sector_scratch_idx = 0;
        
        // Find new beam - same as original
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
        
        if (use_cache)
        {
            cache_hitnum += static_cast<int>(cached_nhoods.size());
        }

        // Read nhoods of frontier ids - ASYNC VERSION
        if (!frontier.empty()) {
            if (stats != nullptr) {
                stats->n_hops++;
            }
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
            
            // Perform async reads - THE MAIN DIFFERENCE FROM ORIGINAL
            io_timer.reset();
            if (!async_reader) {
                throw std::runtime_error("Async reader not available");
            }
            co_await async_reader->async_read_coro(frontier_read_reqs);
            if (stats != nullptr) {
                stats->io_us += (float)io_timer.elapsed();
            }
        }
        
        // Process cached nhoods - exactly same as original
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
            full_retset.push_back(Neighbor((uint32_t)cached_nhood.first, cur_expanded_dist, true));
            
            uint64_t nnbrs = cached_nhood.second.first;
            uint32_t *node_nbrs = cached_nhood.second.second;
            
            // Compute node_nbrs <-> query dists in PQ space - same as original
            cpu_timer.reset();
            compute_dists(node_nbrs, nnbrs, dist_scratch);
            if (stats != nullptr) {
                stats->n_cmps += (uint32_t)nnbrs;
                stats->cpu_us += (float)cpu_timer.elapsed();
            }
            
            // Process prefetched nhood - same as original
            for (uint64_t m = 0; m < nnbrs; ++m) {
                uint32_t id = node_nbrs[m];
                if (visited.insert(id).second) {
                    if (!use_filter && this->_dummy_pts.find(id) != this->_dummy_pts.end())
                        continue;
                    
                    if (use_filter && !(this->point_has_label(id, filter_label)) &&
                        (!this->_use_universal_label || !this->point_has_label(id, this->_universal_filter_label)))
                        continue;
                    
                    cmps++;
                    float dist = dist_scratch[m];
                    Neighbor nn(id, dist);
                    retset.insert(nn);
                }
            }
        }
        
        // Process each frontier nhood - same as original except no USE_BING_INFRA
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
            full_retset.push_back(Neighbor(frontier_nhood.first, cur_expanded_dist, true));
            
            uint32_t *node_nbrs = (node_buf + 1);
            // Compute node_nbrs <-> query dist in PQ space - same as original
            cpu_timer.reset();
            compute_dists(node_nbrs, nnbrs, dist_scratch);
            if (stats != nullptr) {
                stats->n_cmps += (uint32_t)nnbrs;
                stats->cpu_us += (float)cpu_timer.elapsed();
            }
            
            cpu_timer.reset();
            // Process prefetched nhood - same as original
            for (uint64_t m = 0; m < nnbrs; ++m) {
                uint32_t id = node_nbrs[m];
                if (visited.insert(id).second) {
                    if (!use_filter && this->_dummy_pts.find(id) != this->_dummy_pts.end())
                        continue;
                    
                    if (use_filter && !(this->point_has_label(id, filter_label)) &&
                        (!this->_use_universal_label || !this->point_has_label(id, this->_universal_filter_label)))
                        continue;
                    
                    cmps++;
                    float dist = dist_scratch[m];
                    if (stats != nullptr) {
                        stats->n_cmps++;
                    }
                    
                    Neighbor nn(id, dist);
                    retset.insert(nn);
                }
            }
            
            if (stats != nullptr)
            {
                stats->cpu_us += (float)cpu_timer.elapsed();
            }
        }

        cur_list_size = static_cast<unsigned>(retset.size());
        hops++;
        step_num++;

        if (FULL_RETSET_SWAP)
        {
            std::sort(full_retset.begin(), full_retset.end());
            if (step_num > (k_search / beam_width + 1))
            {
                for (size_t i = 0; i < full_retset.size(); i++)
                {
                    full_retset[i].remain_step++;
                    full_retset[i].remain_pos += static_cast<unsigned>(i);
                }
            }
        }

        if (stats != nullptr && gt_flag)
        {
            if (FULL_RETSET_SWAP)
            {
                float stable = 0.0f;
                for (size_t i = 0; i < full_retset.size() && i < k_search; i++)
                {
                    if (full_retset[i].last_pos == i)
                    {
                        stable += 1.0f;
                    }
                    else
                    {
                        break;
                    }
                }

                int addnum = 0;
                bool changed = false;
                if (stable > 0.33f * static_cast<float>(k_search) && step_num == next_step_po)
                {
                    float old_award = award;
                    if (!full_retset_final_ids.empty())
                    {
                        award = 0.0f;
                        size_t remove_num = 0;
                        for (size_t i = 0; i < full_retset_final_ids.size(); i++)
                        {
                            bool mismatch = (i >= full_retset.size()) ||
                                            (full_retset_final_ids[i] != full_retset[i].id);
                            if (mismatch)
                            {
                                prefetch_over = false;
                                full_retset_final_ids.erase(full_retset_final_ids.begin() + static_cast<long>(i - remove_num));
                                full_retset_final.erase(full_retset_final.begin() + static_cast<long>(i - remove_num));
                                remove_num++;
                                changed = true;
                            }
                        }
                        award = 0.5f * (stable - static_cast<float>(4 * remove_num)) + 0.5f * old_award;
                    }

                    addnum = static_cast<int>((stable - static_cast<float>(full_retset_final_ids.size()) + award) / 2.0f);

                    for (size_t i = 0; i < full_retset.size() && i < k_search && addnum > 0; i++)
                    {
                        if (full_retset[i].remain_step > (k_search / beam_width + 1) && full_retset[i].last_pos == i)
                        {
                            if (std::find(full_retset_final_ids.begin(), full_retset_final_ids.end(), full_retset[i].id) ==
                                full_retset_final_ids.end())
                            {
                                full_retset_final_ids.push_back(full_retset[i].id);
                                full_retset_final.push_back(
                                    Neighbor(full_retset[i].id, full_retset[i].distance, full_retset[i].flag));
                                addnum--;
                                changed = true;
                            }
                        }

                        if (full_retset_final_ids.size() >= k_search)
                        {
                            if (prefetch_over)
                            {
                                force_terminate = true;
                            }
                            else
                            {
                                prefetch_over = true;
                            }
                            break;
                        }
                    }
                }

                if (changed && streaming_enabled && false)
                {
                    if (use_cache)
                        std::cout << stats->n_ios + cache_hitnum / 2 << "," << full_retset_final_ids.size() << "," << std::endl;
                    else
                        std::cout << stats->n_ios << "," << full_retset_final_ids.size() << "," << std::endl;
                }

                if (step_num == next_step_po)
                {
                    double fraction = (k_search == 0) ? 0.0 : (static_cast<double>(full_retset_final_ids.size()) / k_search);
                    next_step_po = static_cast<unsigned>(step_num + fraction * full_retset_final_ids.size() + 1);
                    if (award < 0)
                    {
                        next_step_po += static_cast<unsigned>(-award);
                    }
                    updata_lastpos += 1;
                    if (updata_lastpos % 1 == 0)
                    {
                        for (size_t i = 0; i < full_retset.size(); i++)
                        {
                            full_retset[i].last_pos = static_cast<unsigned>(i);
                        }
                    }
                }
            }
            for (int i = 0; i < HRS_LEN; i++)
            {
                register_hitnum_reset_nn(hrs[i], stats->hitnum_reset_n[i], stats->hitnum_reset_n_split[i],
                                         stats->time_reset_n[i], stats->io_reset_n[i],
                                         stats->hitnum_reset_n_final[i], stats->len_frf_n[i]);
            }
        }

        if (streaming_enabled && multi_stage_streaming && !stage_one_emitted && stage_one_target > 0)
        {
            bool enough_results = full_retset_final.size() >= stage_one_target;
            bool io_ready = (min_ios_before_emit == 0) || (num_ios >= min_ios_before_emit);
            bool step_ready = (min_steps_before_emit == 0) || (step_num >= min_steps_before_emit);

            if (enough_results && io_ready && step_ready)
            {
                emit_stage(0, false, full_retset_final, stage_one_target);
                stage_one_emitted = true;
            }
        }

        if (force_terminate)
        {
            break;
        }
    }
    
    if (stats != nullptr && gt_flag)
    {
        for (int i = 0; i < HRS_LEN; i++)
        {
            register_hitnum_reset_nn_after(hrs[i], stats->hitnum_reset_n[i], stats->hitnum_reset_n_split[i],
                                           stats->time_reset_n[i], stats->io_reset_n[i],
                                           stats->hitnum_reset_n_final[i], stats->len_frf_n[i]);
        }
    }

    // Re-sort by distance - same as original
    std::sort(full_retset.begin(), full_retset.end());

    // print full_retset && full_retset_final
    // if (streaming_enabled) {
    //     std::cout << "Full Retset IDs: ";
    //     for (const auto &nb : full_retset) {
    //         std::cout << nb.id << " ";
    //     }
    //     std::cout << std::endl;
    //     std::cout << "Full Retset Final IDs: ";
    //     for (const auto &nb : full_retset_final) {
    //         std::cout << nb.id << " ";
    //     }
    //     std::cout << std::endl;
    // }

    full_retset_final.clear();
    full_retset_final_ids.clear();

    if (FULL_RETSET_SWAP)
    {
        for (auto &nb : full_retset)
        {
            if (full_retset_final_ids.size() >= k_search)
            {
                break;
            }
            if (std::find(full_retset_final_ids.begin(), full_retset_final_ids.end(), nb.id) ==
                full_retset_final_ids.end())
            {
                full_retset_final_ids.push_back(nb.id);
                full_retset_final.push_back(nb);
            }
        }
        full_retset_final.swap(full_retset);
    }

    // if (streaming_enabled) {
    //     // print Swapped full_retset_final
    //     std::cout << "Swapped Full Retset Final IDs: ";
    //     for (const auto &nb : full_retset_final) {
    //         std::cout << nb.id << " ";
    //     }
    //     std::cout << std::endl;
    // }
    
    if (use_reorder_data) {
        if (!(this->_reorder_data_exists)) {
            throw ANNException("Requested use of reordering data which does not exist in index file",
                             -1, __FUNCSIG__, __FILE__, __LINE__);
        }

        std::vector<AlignedRead> vec_read_reqs;

        // Limit full_retset size - same as original
        if (full_retset.size() > k_search * FULL_PRECISION_REORDER_MULTIPLIER) {
            full_retset.erase(full_retset.begin() + k_search * FULL_PRECISION_REORDER_MULTIPLIER, full_retset.end());
        }

        for (size_t i = 0; i < full_retset.size(); ++i) {
            // Use VECTOR_SECTOR_NO macro - same as original
            vec_read_reqs.emplace_back(
                VECTOR_SECTOR_NO(full_retset[i].id) * defaults::SECTOR_LEN,
                defaults::SECTOR_LEN, 
                sector_scratch + i * defaults::SECTOR_LEN);

            if (stats != nullptr) {
                stats->n_4k++;
                stats->n_ios++;
            }
        }

        // Perform async reads for reordering data - THE MAIN DIFFERENCE FROM ORIGINAL
        io_timer.reset();
        if (!async_reader) {
            throw std::runtime_error("Async reader not available for reordering");
        }
        co_await async_reader->async_read_coro(vec_read_reqs);
        if (stats != nullptr) {
            stats->io_us += io_timer.elapsed();
        }

        // Recompute distances with full precision data - same as original
        for (size_t i = 0; i < full_retset.size(); ++i) {
            auto id = full_retset[i].id;
            // Use VECTOR_SECTOR_OFFSET macro - same as original
            auto location = (sector_scratch + i * defaults::SECTOR_LEN) + VECTOR_SECTOR_OFFSET(id);
            full_retset[i].distance = this->_dist_cmp->compare(aligned_query_T, (T *)location, (uint32_t)this->_data_dim);
        }

        // Re-sort with new distances - same as original
        std::sort(full_retset.begin(), full_retset.end());
    }

    if (streaming_enabled)
    {
        std::sort(full_retset.begin(), full_retset.end());
        size_t target_total = std::min<uint64_t>(k_search, full_retset.size());
        emit_stage(final_stage_idx, true, full_retset, target_total);
    }

    // Copy k_search values - same as original (always executed, not in else branch)
    for (uint64_t i = 0; i < k_search; i++) {
        indices[i] = full_retset[i].id;
        auto key = (uint32_t)indices[i];
        if (this->_dummy_pts.find(key) != this->_dummy_pts.end()) {
            indices[i] = this->_dummy_to_real_map[key];
        }

        if (distances != nullptr) {
            distances[i] = full_retset[i].distance;
            if (this->metric == diskann::Metric::INNER_PRODUCT) {
                // Flip the sign to convert min to max - same as original
                distances[i] = (-distances[i]);
                // Rescale to revert back to original norms - same as original
                if (this->_max_base_norm != 0)
                    distances[i] *= (this->_max_base_norm * query_norm);
            }
        }
    }
    
    if (stats != nullptr) {
        stats->total_us = (float)query_timer.elapsed();
        stats->n_steps = step_num;
        stats->visited_length = static_cast<unsigned>(visited.size());
    }
    
    co_return;
}
// Explicit template instantiations
template class AsyncPQFlashIndex<float>;
template class AsyncPQFlashIndex<int8_t>;
template class AsyncPQFlashIndex<uint8_t>;

template class AsyncPQFlashIndex<float, uint16_t>;
template class AsyncPQFlashIndex<int8_t, uint16_t>;
template class AsyncPQFlashIndex<uint8_t, uint16_t>;

} // namespace diskann
