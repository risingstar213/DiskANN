# DiskANN 协程版本 (Coroutine-based Async DiskANN)

## 概述

这是DiskANN的协程版本实现，通过C++20协程和io_uring实现异步IO，在等待磁盘IO时挂起协程，显著提升IO密集型场景的并发性能。

## 主要特性

### 1. 异步IO架构
- 基于io_uring的高性能异步IO
- 协程调度器管理IO等待和协程恢复
- 在IO等待期间CPU资源不被阻塞

### 2. 协程化搜索
- `AsyncPQFlashIndex`: 支持协程的磁盘索引类
- `async_cached_beam_search`: 异步版本的beam search
- `async_batch_search`: 并发处理多个查询

### 3. 向下兼容
- 保持与原版DiskANN相同的API接口
- 可以通过编译选项启用/禁用协程功能

## 系统要求

- **编译器**: GCC 10.0+ 或 Clang 14.0+
- **C++标准**: C++20
- **依赖**: liburing-dev
- **操作系统**: Linux (io_uring支持)

## 安装依赖

```bash
# Ubuntu/Debian
sudo apt install liburing-dev

# 或者手动编译liburing
git clone https://github.com/axboe/liburing.git
cd liburing
make && sudo make install
```

## 构建

### 快速构建
```bash
./build_async_diskann.sh
```

### 手动构建
```bash
mkdir build_async && cd build_async
cmake .. -DENABLE_COROUTINES=ON -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

## 使用方法

### 异步搜索
```bash
# 与原版search_disk_index相同的参数
./apps/async_search_disk_index \\
    --data_type float \\
    --dist_fn l2 \\
    --index_path_prefix /path/to/index \\
    --result_path /path/to/results \\
    --query_file /path/to/queries.bin \\
    --recall_at 10 \\
    --search_list 50 100 200 \\
    --num_threads 16 \\
    --beamwidth 4
```

### 性能测试
```bash
# 运行基本测试
./apps/test_coroutine_diskann

# 对比原版和协程版本性能
time ./apps/search_disk_index [args...]      # 原版
time ./apps/async_search_disk_index [args...] # 协程版
```

## 架构设计

### 核心组件

1. **CoroutineScheduler** (`include/coroutine_scheduler.h`)
   - 基于io_uring的异步IO调度器
   - 管理协程的挂起和恢复
   - 提供`IOAwaitable`用于异步等待

2. **AsyncLinuxAlignedFileReader** (`include/async_linux_aligned_file_reader.h`)
   - 替换`LinuxAlignedFileReader`的异步版本
   - 提供协程友好的`async_read_coro()`方法
   - 向下兼容同步API

3. **AsyncPQFlashIndex** (`include/async_pq_flash_index.h`)
   - 继承自`PQFlashIndex`的异步版本
   - 实现`async_cached_beam_search()`协程方法
   - 支持批量异步搜索`async_batch_search()`

### 工作流程

```
查询请求 -> 协程调度器 -> AsyncPQFlashIndex
                    ↓
            async_cached_beam_search
                    ↓
        准备IO请求 -> io_uring提交 -> 协程挂起
                    ↓
            IO完成回调 -> 协程恢复 -> 处理结果
                    ↓
                返回搜索结果
```

### 性能优势

1. **IO并发**: 多个查询可以并发执行IO操作
2. **CPU利用率**: IO等待期间CPU处理其他协程
3. **内存效率**: 协程栈比线程栈更轻量
4. **扩展性**: 单线程支持大量并发查询

## 性能对比

理论上协程版本在以下场景有显著优势:

- **IO密集型负载**: 大量随机磁盘访问
- **高并发查询**: 同时处理多个搜索请求
- **SSD存储**: io_uring对NVMe SSD优化明显
- **大索引**: 缓存命中率低，IO等待时间长

预期性能提升:
- QPS: 2-5x提升 (取决于IO等待时间)
- 延迟: 在高负载下延迟更稳定
- 资源利用: CPU利用率提升20-40%

## 代码示例

### 基本协程搜索
```cpp
#include "async_pq_flash_index.h"
#include "coroutine_scheduler.h"

diskann::Task<void> search_example() {
    auto index = diskann::create_async_pq_flash_index<float>();
    index->load(num_threads, index_prefix);
    
    std::vector<uint64_t> indices(10);
    std::vector<float> distances(10);
    
    co_await index->async_cached_beam_search(
        query, 10, 100, indices.data(), distances.data(), 4);
    
    // 处理结果...
}
```

### 批量异步搜索
```cpp
diskann::Task<void> batch_search_example() {
    std::vector<std::vector<uint64_t>> all_indices;
    std::vector<std::vector<float>> all_distances;
    
    co_await index->async_batch_search(
        queries, num_queries, query_dim,
        k, l, all_indices, all_distances, beam_width);
    
    // 处理所有结果...
}
```

## 已知限制

1. **平台**: 仅支持Linux (需要io_uring)
2. **编译器**: 需要C++20协程支持
3. **内存**: 协程版本内存使用稍高
4. **调试**: 协程调试相对复杂

## 故障排除

### 编译错误
- 确保编译器支持C++20协程
- 检查liburing是否正确安装
- 使用`-DENABLE_COROUTINES=ON`启用协程

### 运行错误  
- 检查io_uring内核支持 (`/proc/sys/kernel/io_uring_disabled`)
- 确保有足够的内存用于协程栈
- 检查文件权限和磁盘空间

### 性能问题
- 调整`MAX_ENTRIES`参数 (默认1024)
- 确保SSD支持高并发IO
- 监控CPU和IO利用率

## 贡献

欢迎提交Issue和Pull Request来改进协程版本：

- 性能优化建议
- bug修复
- 新功能添加
- 文档改进

## 许可证

与DiskANN主项目相同，使用MIT许可证。
