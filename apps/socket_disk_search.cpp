#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <sys/socket.h>
#include <sys/unistd.h>
#include <sys/types.h>
#include <sys/errno.h>
#include <netinet/in.h>
#include <signal.h>

#include <atomic>

#include <iomanip>
#include <omp.h>
#include <pq_flash_index.h>
#include <set>
#include <string.h>
#include <time.h>
#include <boost/program_options.hpp>

#include "timer.h"
#include "utils.h"
#include "percentile_stats.h"

#ifndef _WINDOWS
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include "linux_aligned_file_reader.h"
#else
#ifdef USE_BING_INFRA
#include "bing_aligned_file_reader.h"
#else
#include "windows_aligned_file_reader.h"
#endif
#endif

#define BUFF_LEN 16384     // 16384/128=128, 16384/768=21.333
#define DEFAULT_PORT 8080  // 指定端口为16555
#define MAXLINK 128
int sockfd, connfd;  // 定义服务端套接字和客户端套接字

// #define DATA_TYPR_NAME "float"
// #define DATA_TYPR float
// #define DISK_FN "l2"
// #define INDEX_PATH_PREFIX "data/yelp/disk_index_yelp_base_R64_L100_A1.2"
// #define BEAMWIDTH 2
// #define VECTOR_DIM 768
// #define VDB_NUM_THREADS 16
// #define NUM_NODE_TO_CACHE 0 // 性能太好，暂时不设置缓存
// #define SEARCHLIST_LEN_MULTIPLE 30 //
// 搜索队列长度等于topk*SEARCHLIST_LEN_MULTIPLE #define USE_REORDER_DATA false
// #define ID_LENGTH 6 // yelp数据集有650000行，最大下标6位数

#define DATA_TYPR_NAME "float"
#define DATA_TYPR float
#define DISK_FN "l2"
#define INDEX_PATH_PREFIX \
  "/mnt/dataset/wiki_dpr_copy/disk_index_wiki_dpr_base_R128_L256_A1.2"
#define BEAMWIDTH 64
#define VECTOR_DIM 768
#define VDB_NUM_THREADS 1
#define NUM_NODE_TO_CACHE 0  // 性能太好，暂时不设置缓存
#define SEARCHLIST_LEN_MULTIPLE \
  40  // 搜索队列长度等于topk*SEARCHLIST_LEN_MULTIPLE
#define USE_REORDER_DATA false
#define ID_LENGTH 8  // wiki_dpr数据集有1000 0000+行，最大下标6位数

void stopServerRunning(int p) {
  close(sockfd);
  printf("Close Server\n");
  exit(0);
}

int main() {
  if (DATA_TYPR_NAME != std::string("float")) {
    std::cerr << "Unsupported data type. Use float!" << std::endl;
    return -1;
  }

  diskann::Metric metric;
  if (DISK_FN == std::string("mips")) {
    metric = diskann::Metric::INNER_PRODUCT;
  } else if (DISK_FN == std::string("l2")) {
    metric = diskann::Metric::L2;
  } else if (DISK_FN == std::string("cosine")) {
    metric = diskann::Metric::COSINE;
  } else {
    std::cout << "Unsupported distance function. Currently only L2/ Inner "
                 "Product/Cosine are supported."
              << std::endl;
    return -1;
  }

  if ((DATA_TYPR_NAME != std::string("float")) &&
      (metric == diskann::Metric::INNER_PRODUCT)) {
    std::cout << "Currently support only floating point data for Inner Product."
              << std::endl;
    return -1;
  }
  if (USE_REORDER_DATA && DATA_TYPR_NAME != std::string("float")) {
    std::cout << "Error: Reorder data for reordering currently only "
                 "supported for float data type."
              << std::endl;
    return -1;
  }

  diskann::cout << "Search parameters: #threads: " << VDB_NUM_THREADS << ", ";
  if (BEAMWIDTH <= 0) {
    diskann::cout << "beamwidth to be optimized for each L value" << std::endl;
    return -1;
  } else {
    diskann::cout << " beamwidth: " << BEAMWIDTH << std::endl;
  }
  std::shared_ptr<AlignedFileReader> reader =
      nullptr;  // 初始化用于读取原数据文件的reader，该reader可被共享（如构造PQFlashIndex时通过传参共享给PQFlashIndex）
#ifdef _WINDOWS
#ifndef USE_BING_INFRA
  reader.reset(new WindowsAlignedFileReader());
#else
  reader.reset(new diskann::BingAlignedFileReader());
#endif
#else
  reader.reset(new LinuxAlignedFileReader());
#endif

  std::unique_ptr<diskann::PQFlashIndex<DATA_TYPR>>
      _pFlashIndex(  // 构建并初始化 pq索引
          new diskann::PQFlashIndex<DATA_TYPR>(reader, metric));

  std::string index_path_prefix = INDEX_PATH_PREFIX;
  int         res = _pFlashIndex->load(
      VDB_NUM_THREADS,
      index_path_prefix.c_str());  // 加载原数据，并通过num_threads设置reader

  if (res != 0) {
    return res;
  }
  // cache bfs levels 加载缓存
  std::vector<uint32_t> node_list;
  diskann::cout << "Caching " << NUM_NODE_TO_CACHE
                << " BFS nodes around medoid(s)" << std::endl;
  //_pFlashIndex->cache_bfs_levels(NUM_NODE_TO_CACHE, node_list);
  std::string warmup_query_file = index_path_prefix + "_sample_data.bin";
  if (NUM_NODE_TO_CACHE > 0)
    _pFlashIndex
        ->generate_cache_list_from_sample_queries(  // 从原数据的切片集合sample中计算缓存节点的id并存到node_list
            warmup_query_file, 15, 6, NUM_NODE_TO_CACHE, VDB_NUM_THREADS,
            node_list);
  _pFlashIndex->load_cache_list(node_list);  // 根据nodelist加载缓存
  node_list.clear();
  node_list.shrink_to_fit();

  omp_set_num_threads(VDB_NUM_THREADS);  // 设置即将出现的并行区域中的线程数，除非由
                                         // num_threads 子句重写

  diskann::cout.setf(std::ios_base::fixed, std::ios_base::floatfield);
  diskann::cout.precision(2);

  // 初始化socket部分 ---------------------------- START
  struct sockaddr_in servaddr;        // 用于存放ip和端口的结构
  char               buff[BUFF_LEN];  // 用于收发数据
  sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (-1 == sockfd) {
    printf("Create socket error(%d): %s\n", errno, strerror(errno));
    return -1;
  }

  bzero(&servaddr, sizeof(servaddr));
  servaddr.sin_family = AF_INET;
  servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
  servaddr.sin_port = htons(DEFAULT_PORT);

  if (-1 == bind(sockfd, (struct sockaddr*) &servaddr, sizeof(servaddr))) {
    printf("Bind error(%d): %s\n", errno, strerror(errno));
    return -1;
  }

  if (-1 == listen(sockfd, MAXLINK)) {
    printf("Listen error(%d): %s\n", errno, strerror(errno));
    return -1;
  }
  printf("Listening...\n");
  // 初始化socket部分 ---------------------------- END

  while (true) {
    signal(SIGINT, stopServerRunning);  // 这句用于在输入Ctrl+C的时候关闭服务器
    connfd = accept(sockfd, NULL, NULL);
    if (-1 == connfd) {
      printf("Accept error(%d): %s\n", errno, strerror(errno));
      return -1;
    } else {
      printf("\n【sever】connection No.%d\n", connfd);
    }

    // 接收原数据
    bzero(buff, BUFF_LEN);
    recv(connfd, buff, BUFF_LEN - 1, 0);
    // meta_data : topk,input_query_num,len(prompt)
    // 当前仅支持一次对一个promt进行查询
    char  delims[] = ",";
    char* result = NULL;
    result = strtok(buff, delims);
    std::string topk_str(result);
    int         topk = stoi(topk_str);
    result = strtok(NULL, delims);
    std::string input_query_num_str(result);
    int         input_query_num = stoi(input_query_num_str);
    result = strtok(NULL, delims);
    std::string length_str(result);
    int         recv_length = stoi(length_str);
    printf("【sever】meta datas: topk=%d input_query_num=%d recv_length=%d\n",
           topk, input_query_num, recv_length);

    char buff1[] = "Go";
    send(connfd, buff1, strlen(buff1), 0);

    // 接收prompt对应的句向量
    bzero(buff, BUFF_LEN);
    recv(connfd, buff, BUFF_LEN - 1, 0);
    buff[BUFF_LEN - 1] = '\0';
    std::string query_vector_str(buff);
    printf("query_vector_str.length()=%d", (int)query_vector_str.length());
    while (query_vector_str.length() < recv_length) {
      bzero(buff, BUFF_LEN);
      recv(connfd, buff, BUFF_LEN - 1, 0);
      buff[BUFF_LEN - 1] = '\0';
      query_vector_str += buff;
      printf("  ...=%d", (int)query_vector_str.length());
    }
    // std::cout<<"\n query_vector_str:"<<query_vector_str<<std::endl;

    // VDB查询开始（当前为一次查询完毕）
    printf("【sever】search start\n");
    size_t query_num = input_query_num;
    int*   final_result_ids = new int[query_num * topk];
    // 还需要进一步解析query
    DATA_TYPR* query = nullptr;
    size_t     query_aligned_dim;
    diskann::load_aligned_bin_mem<DATA_TYPR>(
        query_vector_str, query, query_num, VECTOR_DIM,
        query_aligned_dim);  // 加载搜索请求

    diskann::cout << std::setw(6) << "L" << std::setw(12) << "Beamwidth"
                  << std::setw(16) << "QPS" << std::setw(16) << "Mean Latency"
                  << std::setw(16) << "99.9 Latency" << std::setw(16)
                  << "Mean IOs" << std::setw(16) << "CPU (s)" << std::endl;
    diskann::cout
        << "==============================================================="
           "======================================================="
        << std::endl;

    std::vector<std::vector<uint32_t>> query_result_ids(1);
    std::vector<std::vector<float>>    query_result_dists(1);

    uint32_t optimized_beamwidth = 2;

    // 为减少代码改动，暂未取消 for循环
    for (uint32_t test_id = 0; test_id < 1;
         test_id++) {  // SEARCHLIST_LEN是搜索列表长度
      __u64 L = topk * SEARCHLIST_LEN_MULTIPLE;

      if (L < topk) {
        diskann::cout << "Ignoring search with L:" << L
                      << " since it's smaller than K:" << topk << std::endl;
        return -1;
      }

      if (BEAMWIDTH <= 0) {
        diskann::cout << "Tuning BEAMWIDTH.." << std::endl;
        return -1;
      } else
        optimized_beamwidth = BEAMWIDTH;

      query_result_ids[test_id].resize(topk * query_num);  // 结果的id集合
      query_result_dists[test_id].resize(topk * query_num);  // 结果的距离集合

      auto stats = new diskann::QueryStats[query_num];

      std::vector<uint64_t> query_result_ids_64(topk * query_num);
      auto                  s = std::chrono::high_resolution_clock::now();

#pragma omp parallel for schedule(dynamic, 1)  // 使用omp并行处理for循环
      for (__s64 i = 0; i < (int64_t) query_num; i++) {  // 处理每个query
        _pFlashIndex->cached_beam_search(
            query + (i * query_aligned_dim), topk, L,
            query_result_ids_64.data() + (i * topk),
            query_result_dists[test_id].data() + (i * topk),
            optimized_beamwidth, USE_REORDER_DATA, stats + i);
      }
      auto e = std::chrono::high_resolution_clock::now();
      std::chrono::duration<double> diff = e - s;
      float qps = (1.0 * query_num) / (1.0 * diff.count());

      diskann::convert_types<uint64_t, uint32_t>(
          query_result_ids_64.data(), query_result_ids[test_id].data(),
          query_num, topk);
      // 统计结果输出
      auto mean_latency = diskann::get_mean_stats<float>(
          stats, query_num,
          [](const diskann::QueryStats& stats) { return stats.total_us; });

      auto latency_999 = diskann::get_percentile_stats<float>(
          stats, query_num, 0.999,
          [](const diskann::QueryStats& stats) { return stats.total_us; });

      auto mean_ios = diskann::get_mean_stats<unsigned>(
          stats, query_num,
          [](const diskann::QueryStats& stats) { return stats.n_ios; });

      auto mean_cpuus = diskann::get_mean_stats<float>(
          stats, query_num,
          [](const diskann::QueryStats& stats) { return stats.cpu_us; });

      float recall = 0;

      diskann::cout << std::setw(6) << L << std::setw(12) << optimized_beamwidth
                    << std::setw(16) << qps << std::setw(16) << mean_latency
                    << std::setw(16) << latency_999 << std::setw(16) << mean_ios
                    << std::setw(16) << mean_cpuus << std::endl;
      delete[] stats;
    }

    std::string final_result_ids_str = "";
    for (int i = 0; i < query_result_ids[0].size(); ++i) {
      final_result_ids_str =
          final_result_ids_str + std::to_string(query_result_ids[0][i]) + ",";
    }
    std::cout << "【sever】get final ids:" << final_result_ids_str << std::endl;

    diskann::aligned_free(query);
    printf("【sever】search over\n");

    // 继续socket部分
    char* buff2 =
        new char[topk * query_num *
                 (ID_LENGTH +
                  1)];  // yelp数据集有650000行，最大下标6位数，加上一个分隔符
    final_result_ids_str.copy(
        buff2, final_result_ids_str.length());  // 裁剪末尾的无效字符
    buff2[final_result_ids_str.length()] = '\0';
    send(connfd, buff2, strlen(buff2), 0);

    // bzero(buff, BUFF_LEN);
    // recv(connfd, buff, BUFF_LEN - 1, 0);
    // printf("【sever】go?? : %s\n", buff);
    // char buff3[] = "【sever】CTX2.................";
    // send(connfd, buff3, strlen(buff3), 0);

    bzero(buff, BUFF_LEN);
    recv(connfd, buff, BUFF_LEN - 1, 0);
    printf("【sever】over?? : %s\n", buff);

    close(connfd);
    delete[] final_result_ids;
    delete[] buff2;
  }
  return 0;
}
