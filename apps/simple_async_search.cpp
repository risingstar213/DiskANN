// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "common_includes.h"

#include "async_pq_flash_index.h"
#include "disk_utils.h"

// 简化的协程版本，避免编译器内部错误
template <typename T, typename LabelT = uint32_t>
diskann::Task<int> simple_async_search(const std::string &index_path_prefix,
                                       const std::string &query_file,
                                       uint32_t num_threads,
                                       uint32_t L,
                                       uint32_t num_queries_to_search = 100) {
    try {
        diskann::cout << "简化协程搜索开始..." << std::endl;
        
        // 创建异步索引
        auto async_index = diskann::create_async_pq_flash_index<T, LabelT>(diskann::Metric::L2);
        
        // 加载索引
        int res = async_index->load(num_threads, index_path_prefix.c_str());
        if (res != 0) {
            diskann::cout << "索引加载失败" << std::endl;
            co_return res;
        }
        
        diskann::cout << "索引加载成功" << std::endl;
        
        // 加载查询数据
        T *query = nullptr;
        size_t query_num, query_dim, query_aligned_dim;
        
        diskann::load_aligned_bin<T>(query_file, query, query_num, query_dim, query_aligned_dim);
        
        diskann::cout << "查询数据: " << query_num << " 个查询, 维度: " << query_dim << std::endl;
        
        // 限制查询数量避免过度复杂
        uint32_t actual_queries = std::min((uint32_t)query_num, num_queries_to_search);
        
        // 准备结果存储
        std::vector<uint64_t> indices_buffer(actual_queries * L);
        std::vector<float> distances_buffer(actual_queries * L);
        
        auto start_time = std::chrono::high_resolution_clock::now();
        
        // 执行协程搜索
        for (uint32_t i = 0; i < actual_queries; i++) {
            co_await async_index->async_cached_beam_search(
                query + i * query_aligned_dim,    // query
                L,                                 // k_search  
                L,                                 // l_search
                indices_buffer.data() + i * L,     // indices
                distances_buffer.data() + i * L,   // distances
                4,                                 // beam_width
                false,                             // use_filter
                LabelT{},                          // filter_label
                std::numeric_limits<uint32_t>::max(), // io_limit
                false,                             // use_reorder_data
                nullptr                            // stats
            );
            
            if ((i + 1) % 10 == 0) {
                diskann::cout << "已完成 " << (i + 1) << " 个查询" << std::endl;
            }
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto search_time = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
        
        diskann::cout << "协程搜索完成!" << std::endl;
        diskann::cout << "总时间: " << search_time / 1000.0 << " ms" << std::endl;
        diskann::cout << "平均每查询: " << search_time / (1000.0 * actual_queries) << " ms" << std::endl;
        diskann::cout << "QPS: " << (actual_queries * 1000000.0) / search_time << std::endl;
        
        // 显示部分结果
        diskann::cout << "前5个查询的结果:" << std::endl;
        for (uint32_t i = 0; i < std::min(5u, actual_queries); i++) {
            diskann::cout << "Query " << i << ": ";
            for (uint32_t j = 0; j < std::min(5u, L); j++) {
                diskann::cout << indices_buffer[i * L + j] << "(" 
                             << distances_buffer[i * L + j] << ") ";
            }
            diskann::cout << std::endl;
        }
        
        diskann::aligned_free(query);
        co_return 0;
        
    } catch (const std::exception& e) {
        diskann::cout << "协程搜索异常: " << e.what() << std::endl;
        co_return -1;
    }
}

// 包装函数运行协程
template <typename T, typename LabelT = uint32_t>
int run_simple_async_search(const std::string &index_path_prefix,
                            const std::string &query_file,
                            uint32_t num_threads,
                            uint32_t L) {
    try {
        auto task = simple_async_search<T, LabelT>(index_path_prefix, query_file, num_threads, L);
        
        // 简单的协程执行器
        while (task.coro && !task.coro.done()) {
            task.coro.resume();
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
        
        return task.coro.promise().exception ? -1 : 0;
        
    } catch (const std::exception& e) {
        diskann::cout << "运行异常: " << e.what() << std::endl;
        return -1;
    }
}

int main(int argc, char **argv) {
    std::string index_path_prefix, query_file;
    uint32_t num_threads = 1;
    uint32_t L = 100;
    
    // 简单的参数解析
    if (argc < 3) {
        std::cout << "用法: " << argv[0] << " <index_prefix> <query_file> [num_threads] [L]" << std::endl;
        return -1;
    }
    
    index_path_prefix = argv[1];
    query_file = argv[2];
    
    if (argc > 3) num_threads = std::atoi(argv[3]);
    if (argc > 4) L = std::atoi(argv[4]);
    
    diskann::cout << "参数设置:" << std::endl;
    diskann::cout << "  索引前缀: " << index_path_prefix << std::endl;
    diskann::cout << "  查询文件: " << query_file << std::endl;
    diskann::cout << "  线程数: " << num_threads << std::endl;
    diskann::cout << "  L值: " << L << std::endl;
    
    // 运行简化的协程搜索
    return run_simple_async_search<float, uint32_t>(index_path_prefix, query_file, num_threads, L);
}
