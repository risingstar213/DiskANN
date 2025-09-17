// 修复原始 async_search_disk_index.cpp 的建议

// 1. 将大的协程函数拆分成多个小函数
template <typename T, typename LabelT = uint32_t>
diskann::Task<int> load_and_prepare_index(diskann::Metric &metric, 
                                          const std::string &index_path_prefix,
                                          uint32_t num_threads,
                                          uint32_t num_nodes_to_cache) {
    // 只负责加载和准备索引
    auto _pFlashIndex = diskann::create_async_pq_flash_index<T, LabelT>(metric);
    
    int res = _pFlashIndex->load(num_threads, index_path_prefix.c_str());
    if (res != 0) {
        co_return res;
    }
    
    std::vector<uint32_t> node_list;
    _pFlashIndex->cache_bfs_levels(num_nodes_to_cache, node_list);
    _pFlashIndex->load_cache_list(node_list);
    
    co_return 0;
}

template <typename T, typename LabelT = uint32_t>  
diskann::Task<float> perform_batch_search(std::shared_ptr<diskann::AsyncPQFlashIndex<T, LabelT>> index,
                                          T* queries, uint32_t query_num, uint32_t query_aligned_dim,
                                          uint32_t L, uint32_t beamwidth, uint32_t search_io_limit) {
    // 只负责执行搜索
    std::vector<uint64_t> indices_buffer(query_num * L);
    std::vector<float> distances_buffer(query_num * L);
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    for (uint32_t i = 0; i < query_num; i++) {
        co_await index->async_cached_beam_search(
            queries + i * query_aligned_dim,
            L, L,
            indices_buffer.data() + i * L,
            distances_buffer.data() + i * L,
            beamwidth, false, LabelT{}, search_io_limit, false, nullptr
        );
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    float search_time = std::chrono::duration_cast<std::chrono::microseconds>(
        end_time - start_time).count() / 1000.0f;
    
    co_return search_time;
}

// 2. 主协程函数变得简单
template <typename T, typename LabelT = uint32_t>
diskann::Task<int> async_search_disk_index_fixed(/* 参数列表 */) {
    // 步骤1: 加载索引
    co_await load_and_prepare_index<T, LabelT>(metric, index_path_prefix, num_threads, num_nodes_to_cache);
    
    // 步骤2: 加载查询数据
    T *query = nullptr;
    uint32_t query_num, query_dim, query_aligned_dim;
    diskann::load_aligned_bin<T>(query_file, query, query_num, query_dim, query_aligned_dim);
    
    // 步骤3: 执行搜索
    float search_time = co_await perform_batch_search(index, query, query_num, query_aligned_dim, 
                                                     L, beamwidth, search_io_limit);
    
    // 步骤4: 输出结果
    diskann::cout << "搜索完成，耗时: " << search_time << " ms" << std::endl;
    
    co_return 0;
}
