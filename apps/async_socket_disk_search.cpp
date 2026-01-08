// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "async_pq_flash_index.h"
#include "coroutine_scheduler.h"
#include "logger.h"
#include "percentile_stats.h"
#include "utils.h"
#include "socket_streaming_writer.h"

#ifndef _WINDOWS
#include <arpa/inet.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#else
#error "async_socket_disk_search is only supported on POSIX platforms"
#endif

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <cstdint>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include <omp.h>

namespace {

constexpr std::size_t kBufferLength = 16384;
constexpr uint16_t kDefaultPort = 8080;
constexpr int kMaxPendingConnections = 128;

constexpr std::string_view kDataTypeName{"float"};
using DataType = float;
constexpr std::string_view kDistanceFunction{"l2"};
constexpr std::string_view kIndexPathPrefix{
    "/mnt/dataset/wiki_dpr_new/disk_index_wiki_dpr_base_R128_L256_A1.2"};
constexpr std::string_view kBackgroundQueryPath{
  "/mnt/dataset/question_total/question_total.fbin"};

constexpr uint32_t kBeamWidth = 64;
constexpr uint32_t kVectorDim = 768;
constexpr uint32_t kNumThreads = 1;
constexpr uint32_t kNodesToCache = 0;
constexpr uint32_t kSearchListMultiplier = 40;
constexpr bool kUseReorderData = false;
constexpr uint32_t kIdLength = 8;
constexpr uint32_t kCoroutinesPerThread = 150;
constexpr uint32_t kMinConcurrentQueries = kCoroutinesPerThread;

int g_listen_socket = -1;

struct SearchRequest {
  int     topk = 0;
  int     query_count = 0;
  size_t  payload_chars = 0;
};

struct AlignedDeleter {
  void operator()(DataType* ptr) const noexcept {
    if (ptr != nullptr) {
      diskann::aligned_free(ptr);
    }
  }
};

using UniqueAlignedPtr = std::unique_ptr<DataType, AlignedDeleter>;

struct BackgroundQueries {
  UniqueAlignedPtr data{nullptr};
  size_t aligned_dim = 0;
  size_t count = 0;
};

BackgroundQueries g_background_queries;

struct QueryPayload {
  SearchRequest  request{};
  UniqueAlignedPtr query{nullptr};
  size_t         aligned_dim = 0;
};

struct QueryExecutionOutput {
  uint32_t               L = 0;
  double                 qps = 0.0;
  SearchRequest          request{};
  std::vector<uint32_t>  result_ids;
};

class SocketCloser {
 public:
  explicit SocketCloser(int fd) : fd_(fd) {}
  ~SocketCloser() {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

  SocketCloser(const SocketCloser&) = delete;
  SocketCloser& operator=(const SocketCloser&) = delete;

  SocketCloser(SocketCloser&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
  SocketCloser& operator=(SocketCloser&& other) noexcept {
    if (this != &other) {
      release();
      fd_ = other.fd_;
      other.fd_ = -1;
    }
    return *this;
  }

  void release() {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
  }

 private:
  int fd_;
};

std::string sanitize_line(const std::string& input) {
  std::string cleaned = input;
  cleaned.erase(std::remove(cleaned.begin(), cleaned.end(), '\r'), cleaned.end());
  cleaned.erase(std::remove(cleaned.begin(), cleaned.end(), '\n'), cleaned.end());
  cleaned.erase(std::remove(cleaned.begin(), cleaned.end(), '\0'), cleaned.end());
  return cleaned;
}

bool send_all(int fd, const char* data, size_t bytes) {
  size_t total_sent = 0;
  while (total_sent < bytes) {
    ssize_t sent = ::send(fd, data + total_sent, bytes - total_sent, 0);
    if (sent < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (sent == 0) {
      return false;
    }
    total_sent += static_cast<size_t>(sent);
  }
  return true;
}

bool recv_exact(int fd, size_t expected, std::string& output) {
  output.clear();
  output.reserve(expected);
  std::array<char, kBufferLength> buffer{};

  size_t received = 0;
  while (received < expected) {
    const size_t chunk = std::min(buffer.size(), expected - received);
    ssize_t      bytes = ::recv(fd, buffer.data(), chunk, 0);
    if (bytes < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (bytes == 0) {
      return false;
    }
    output.append(buffer.data(), static_cast<size_t>(bytes));
    received += static_cast<size_t>(bytes);
  }

  return true;
}

void stopServerRunning(int) {
  if (g_listen_socket != -1) {
    ::close(g_listen_socket);
    g_listen_socket = -1;
  }
  std::cout << "Close Server" << std::endl;
  std::_Exit(0);
}

diskann::Metric resolve_metric() {
  if (kDistanceFunction == "mips") {
    return diskann::Metric::INNER_PRODUCT;
  }
  if (kDistanceFunction == "l2") {
    return diskann::Metric::L2;
  }
  if (kDistanceFunction == "cosine") {
    return diskann::Metric::COSINE;
  }
  throw std::runtime_error("Unsupported distance function. Use l2/mips/cosine.");
}

void print_search_header() {
  diskann::cout.setf(std::ios_base::fixed, std::ios_base::floatfield);
  diskann::cout.precision(2);
  diskann::cout << std::setw(6) << "L" << std::setw(12) << "Beamwidth"
                << std::setw(16) << "QPS" << std::setw(16) << "Mean Latency"
                << std::setw(16) << "99.9 Latency" << std::setw(16)
                << "Mean IOs" << std::setw(16) << "CPU (s)" << std::endl;
  diskann::cout
      << "==============================================================="
         "======================================================="
      << std::endl;
}

void log_iteration_stats(uint32_t L, double qps,
             std::vector<diskann::QueryStats>& stats) {
  auto mean_latency = diskann::get_mean_stats<float>(
    stats.data(), static_cast<uint64_t>(stats.size()),
      [](const diskann::QueryStats& st) { return st.total_us; });

  auto latency_999 = diskann::get_percentile_stats<float>(
    stats.data(), static_cast<uint64_t>(stats.size()), 0.999,
      [](const diskann::QueryStats& st) { return st.total_us; });

  auto mean_ios = diskann::get_mean_stats<unsigned int>(
    stats.data(), static_cast<uint64_t>(stats.size()),
      [](const diskann::QueryStats& st) { return st.n_ios; });

  auto mean_cpuus = diskann::get_mean_stats<float>(
    stats.data(), static_cast<uint64_t>(stats.size()),
      [](const diskann::QueryStats& st) { return st.cpu_us; });

  diskann::cout << std::setw(6) << L << std::setw(12) << kBeamWidth
                << std::setw(16) << qps << std::setw(16) << mean_latency
                << std::setw(16) << latency_999 << std::setw(16) << mean_ios
                << std::setw(16) << mean_cpuus << std::endl;
}

using AsyncIndexPtr = std::shared_ptr<diskann::AsyncPQFlashIndex<DataType>>;

diskann::Task<void> async_single_search(const AsyncIndexPtr& index,
                const DataType*     query,
                uint32_t            topk,
                uint32_t            L,
                uint64_t*           result_ids,
                float*              result_dists,
                diskann::QueryStats* stats,
                const diskann::SearchStreamOptions* streaming = nullptr) {
  co_await index->async_cached_beam_search(
      query, topk, L, result_ids, result_dists, kBeamWidth, false,
      static_cast<uint32_t>(0), std::numeric_limits<uint32_t>::max(),
  kUseReorderData, stats, streaming);

  diskann::get_cor_scheduler()->mark_done();
  co_return;
}

void validate_configuration(diskann::Metric metric) {
  if (kDataTypeName != "float") {
    throw std::runtime_error("Unsupported data type. Use float!");
  }

  if (metric == diskann::Metric::INNER_PRODUCT && kDataTypeName != "float") {
    throw std::runtime_error(
        "Currently support only floating point data for Inner Product.");
  }

  if (kUseReorderData && kDataTypeName != "float") {
    throw std::runtime_error(
        "Error: Reorder data supported only for float data type.");
  }

  if (kBeamWidth == 0) {
    throw std::runtime_error(
        "Beamwidth must be non-zero. Set kBeamWidth to a positive value.");
  }
}

void log_async_search_parameters() {
  diskann::cout << "Async Search parameters: #threads: " << kNumThreads
                << ", beamwidth: " << kBeamWidth << std::endl;
}

AsyncIndexPtr load_async_index(diskann::Metric metric) {
  std::shared_ptr<AlignedFileReader> reader =
      std::make_shared<AsyncLinuxAlignedFileReader>();
  auto async_index =
      std::make_shared<diskann::AsyncPQFlashIndex<DataType>>(reader, metric);

  int load_result = async_index->load(kNumThreads, kIndexPathPrefix.data(),
                                      kCoroutinesPerThread);
  if (load_result != 0) {
    throw std::runtime_error("Failed to load async index. Error code: " +
                             std::to_string(load_result));
  }

  return async_index;
}

void configure_cache(const AsyncIndexPtr& async_index) {
  std::vector<uint32_t> node_list;
  diskann::cout << "Caching " << kNodesToCache
                << " BFS nodes around medoid(s)" << std::endl;
  if (kNodesToCache > 0) {
    const std::string warmup_query_file =
        std::string(kIndexPathPrefix) + "_sample_data.bin";
    async_index->generate_cache_list_from_sample_queries(
        warmup_query_file, 15, 6, kNodesToCache, kNumThreads, node_list);
  }
  async_index->load_cache_list(node_list);
}

BackgroundQueries load_background_queries() {
  DataType* raw_queries = nullptr;
  size_t    available_queries = 0;
  size_t    dim = 0;
  size_t    aligned_dim = 0;

  diskann::load_aligned_bin<DataType>(std::string(kBackgroundQueryPath),
                                      raw_queries, available_queries, dim,
                                      aligned_dim);

  if (dim != kVectorDim) {
    throw std::runtime_error(
        "Background query dimension mismatch with configured kVectorDim");
  }

  const size_t desired = static_cast<size_t>(kMinConcurrentQueries);
  if (available_queries < desired) {
    throw std::runtime_error(
        "Background query file does not contain enough vectors for backend load");
  }

  DataType* trimmed_queries = nullptr;
  const size_t copy_bytes = desired * aligned_dim * sizeof(DataType);
  diskann::alloc_aligned(reinterpret_cast<void**>(&trimmed_queries),
                         copy_bytes, 8 * sizeof(DataType));
  std::memcpy(trimmed_queries, raw_queries, copy_bytes);
  diskann::aligned_free(raw_queries);

  return BackgroundQueries{UniqueAlignedPtr(trimmed_queries), aligned_dim,
                           desired};
}

void initialize_background_queries() {
  g_background_queries = load_background_queries();
  diskann::cout << "Loaded " << g_background_queries.count
                << " backend queries from " << kBackgroundQueryPath
                << std::endl;
}

int create_listen_socket(uint16_t port) {
  int listen_socket = ::socket(AF_INET, SOCK_STREAM, 0);
  if (listen_socket < 0) {
    std::perror("Create socket error");
    return -1;
  }
  g_listen_socket = listen_socket;

  int opt = 1;
  ::setsockopt(listen_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  sockaddr_in server_addr{};
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
  server_addr.sin_port = htons(port);

  if (::bind(listen_socket, reinterpret_cast<sockaddr*>(&server_addr),
             sizeof(server_addr)) < 0) {
    std::perror("Bind error");
    ::close(listen_socket);
    g_listen_socket = -1;
    return -1;
  }

  if (::listen(listen_socket, kMaxPendingConnections) < 0) {
    std::perror("Listen error");
    ::close(listen_socket);
    g_listen_socket = -1;
    return -1;
  }

  return listen_socket;
}

bool receive_query_payload(int connfd, QueryPayload& payload) {
  constexpr size_t kMetadataBytes = 3 * sizeof(uint32_t);
  std::string metadata_raw;
  if (!recv_exact(connfd, kMetadataBytes, metadata_raw)) {
    std::cout << "[server] failed to receive metadata" << std::endl;
    return false;
  }

  const uint32_t topk = *(uint32_t*)(metadata_raw.data());
  const uint32_t query_count = *(uint32_t*)(metadata_raw.data() + sizeof(uint32_t));
  const uint32_t payload_chars = *(uint32_t*)(metadata_raw.data() + 2 * sizeof(uint32_t));

  if (topk == 0 || query_count == 0 || payload_chars == 0) {
    std::cout << "[server] invalid metadata values" << std::endl;
    return false;
  }

  payload.request.topk = static_cast<int>(topk);
  payload.request.query_count = static_cast<int>(query_count);
  payload.request.payload_chars = static_cast<size_t>(payload_chars);

  std::string query_vector_payload;
  if (!recv_exact(connfd, payload.request.payload_chars,
                  query_vector_payload)) {
    std::cout << "[server] failed to receive query payload" << std::endl;
    return false;
  }
  // std::cout << "[server] received query payload size="
  //           << query_vector_payload.size() << std::endl;

  const size_t expected_bytes =
      query_count * static_cast<size_t>(kVectorDim) * sizeof(DataType);
  if (query_vector_payload.size() != expected_bytes) {
    std::cout << "[server] payload size mismatch expected=" << expected_bytes
              << " actual=" << query_vector_payload.size() << std::endl;
    return false;
  }

  DataType* raw_query = nullptr;
  const size_t query_aligned_dim = ROUND_UP(kVectorDim, 8);
  const size_t alloc_size = query_count * query_aligned_dim * sizeof(DataType);
  diskann::alloc_aligned(reinterpret_cast<void**>(&raw_query), alloc_size,
                         8 * sizeof(DataType));

  const DataType* src_ptr =
      reinterpret_cast<const DataType*>(query_vector_payload.data());
  for (size_t i = 0; i < query_count; ++i) {
    DataType* dst = raw_query + i * query_aligned_dim;
    std::memcpy(dst, src_ptr + i * kVectorDim,
                static_cast<size_t>(kVectorDim) * sizeof(DataType));
    std::memset(dst + kVectorDim, 0,
                (query_aligned_dim - kVectorDim) * sizeof(DataType));
  }

  payload.query.reset(raw_query);
  payload.aligned_dim = query_aligned_dim;
  return true;
}

QueryExecutionOutput execute_queries(const AsyncIndexPtr& async_index,
                                     diskann::CoroutineScheduler* scheduler,
                                     const QueryPayload& payload) {
  const SearchRequest& request = payload.request;

  const uint32_t L = static_cast<uint32_t>(request.topk) *
                     kSearchListMultiplier;
  if (L < static_cast<uint32_t>(request.topk)) {
    throw std::runtime_error("[server] invalid L computed for topk");
  }

  const int primary_queries = request.query_count;
  std::vector<uint64_t> result_ids_primary(
      static_cast<std::size_t>(request.topk) * primary_queries);
  std::vector<float> result_dists_primary(result_ids_primary.size());
  std::vector<diskann::QueryStats> stats_primary(
      static_cast<std::size_t>(primary_queries));

  const int target_concurrency =
      std::max(primary_queries, static_cast<int>(kMinConcurrentQueries));
  const int background_needed = target_concurrency - primary_queries;

  std::vector<uint64_t> result_ids_background;
  std::vector<float> result_dists_background;
  std::vector<diskann::QueryStats> stats_background;

  std::vector<diskann::Task<void>> tasks;
  tasks.reserve(static_cast<std::size_t>(target_concurrency));

  const DataType* base_query = payload.query.get();
  const size_t    aligned_dim = payload.aligned_dim;
  if (g_background_queries.count == 0 || !g_background_queries.data) {
    throw std::runtime_error("Background queries are not initialized");
  }
  const DataType* background_base = g_background_queries.data.get();
  const size_t    background_aligned_dim = g_background_queries.aligned_dim;

  auto enqueue_search = [&](const DataType* query_ptr, uint64_t* id_out,
                            float* dist_out, diskann::QueryStats* stat_out) {
    tasks.emplace_back(async_single_search(async_index, query_ptr,
                                           static_cast<uint32_t>(request.topk),
                                           L, id_out, dist_out, stat_out));
  };

  for (int i = 0; i < primary_queries; ++i) {
    enqueue_search(base_query + static_cast<std::size_t>(i) * aligned_dim,
                   result_ids_primary.data() +
                       static_cast<std::size_t>(i) * request.topk,
                   result_dists_primary.data() +
                       static_cast<std::size_t>(i) * request.topk,
                   stats_primary.data() + i);
  }

  if (background_needed > 0) {
    result_ids_background.resize(
        static_cast<std::size_t>(background_needed) * request.topk);
    result_dists_background.resize(
        static_cast<std::size_t>(background_needed) * request.topk);
    stats_background.resize(static_cast<std::size_t>(background_needed));

    for (int i = 0; i < background_needed; ++i) {
      const size_t source_idx =
        static_cast<size_t>(i) % g_background_queries.count;
      const DataType* background_query =
        background_base + source_idx * background_aligned_dim;
      enqueue_search(
          background_query,
          result_ids_background.data() +
              static_cast<std::size_t>(i) * request.topk,
          result_dists_background.data() +
              static_cast<std::size_t>(i) * request.topk,
          stats_background.data() + i);
    }

    // std::cout << "[server] added " << background_needed
    //           << " background queries to simulate load" << std::endl;
  }

  for (auto& task : tasks) {
    if (task.coro) {
      scheduler->schedule_coroutine(task.coro);
    }
  }

  auto search_start = std::chrono::high_resolution_clock::now();
  scheduler->run();
  for (auto& task : tasks) {
    task.get_result();
  }
  auto search_end = std::chrono::high_resolution_clock::now();

  const double elapsed =
      std::chrono::duration<double>(search_end - search_start).count();
  const int total_queries_executed = primary_queries + background_needed;
  const double qps = (elapsed > 0.0 && total_queries_executed > 0)
                         ? total_queries_executed / elapsed
                         : 0.0;

  // if (background_needed > 0) {
  //   std::vector<diskann::QueryStats> combined_stats;
  //   combined_stats.reserve(static_cast<std::size_t>(total_queries_executed));
  //   combined_stats.insert(combined_stats.end(), stats_primary.begin(),
  //                         stats_primary.end());
  //   combined_stats.insert(combined_stats.end(), stats_background.begin(),
  //                         stats_background.end());
  //   log_iteration_stats(L, qps, combined_stats);
  // } else {
  //   log_iteration_stats(L, qps, stats_primary);
  // }

  std::vector<uint32_t> result_ids_u32(result_ids_primary.size());
  diskann::convert_types<uint64_t, uint32_t>(
      result_ids_primary.data(), result_ids_u32.data(), primary_queries,
      static_cast<uint32_t>(request.topk));

  return QueryExecutionOutput{L, qps, payload.request,
                              std::move(result_ids_u32)};
}

bool execute_streaming_query(const AsyncIndexPtr& async_index,
                             diskann::CoroutineScheduler* scheduler,
                             const QueryPayload& payload,
                             diskann::streaming::StageChunkWriter& writer,
                             diskann::SearchStreamOptions& stream_opts) {
  const SearchRequest& request = payload.request;
  const uint32_t L = static_cast<uint32_t>(request.topk) * kSearchListMultiplier;
  const DataType* base_query = payload.query.get();
  const size_t aligned_dim = payload.aligned_dim;
  const size_t stats_dim = static_cast<size_t>(std::max(1, request.topk));

  if (g_background_queries.count == 0 || !g_background_queries.data) {
    throw std::runtime_error("Background queries are not initialized");
  }
  const DataType* background_base = g_background_queries.data.get();
  const size_t background_aligned_dim = g_background_queries.aligned_dim;

  const int primary_queries = request.query_count;
  const int target_concurrency =
      std::max(primary_queries, static_cast<int>(kMinConcurrentQueries));
  const int background_needed = target_concurrency - primary_queries;

  std::vector<uint64_t> primary_ids(static_cast<std::size_t>(request.topk));
  std::vector<float> primary_dists(primary_ids.size());
  diskann::QueryStats primary_stats{};
  diskann::streaming::QueryStatsActivator primary_stats_guard;
  primary_stats_guard.configure(&primary_stats, 1, stats_dim);

  std::vector<uint64_t> background_ids;
  std::vector<float> background_dists;
  std::vector<diskann::QueryStats> background_stats;
  diskann::streaming::QueryStatsActivator background_stats_guard;
  if (background_needed > 0) {
    background_ids.resize(static_cast<std::size_t>(background_needed) * request.topk);
    background_dists.resize(background_ids.size());
    background_stats.resize(static_cast<std::size_t>(background_needed));
    background_stats_guard.configure(background_stats.data(), background_stats.size(), stats_dim);
  }

  std::vector<diskann::Task<void>> tasks;
  tasks.reserve(static_cast<std::size_t>(target_concurrency));

  auto enqueue_search = [&](const DataType* query_ptr, uint64_t* id_out,
                            float* dist_out, diskann::QueryStats* stat_out,
                            const diskann::SearchStreamOptions* streaming) {
    tasks.emplace_back(async_single_search(async_index, query_ptr,
                                           static_cast<uint32_t>(request.topk),
                                           L, id_out, dist_out, stat_out,
                                           streaming));
  };

  for (int i = 0; i < primary_queries; ++i) {
    const DataType* query_ptr = base_query + static_cast<std::size_t>(i) * aligned_dim;
    diskann::SearchStreamOptions* streaming = (i == 0) ? &stream_opts : nullptr;
    enqueue_search(query_ptr, primary_ids.data(), primary_dists.data(),
                   &primary_stats, streaming);
  }

  for (int i = 0; i < background_needed; ++i) {
    const size_t source_idx =
      static_cast<size_t>(i) % g_background_queries.count;
    const DataType* background_query =
      background_base + source_idx * background_aligned_dim;
    uint64_t* id_out = background_ids.data() + static_cast<std::size_t>(i) * request.topk;
    float* dist_out = background_dists.data() + static_cast<std::size_t>(i) * request.topk;
    enqueue_search(background_query, id_out, dist_out,
                   background_stats.data() + i, nullptr);
  }

  if (background_needed > 0) {
    // std::cout << "[server] added " << background_needed
    //           << " background queries to simulate load" << std::endl;
  }

  for (auto& task : tasks) {
    if (task.coro) {
      scheduler->schedule_coroutine(task.coro);
    }
  }

  auto search_start = std::chrono::high_resolution_clock::now();
  scheduler->run();
  for (auto& task : tasks) {
    task.get_result();
  }
  auto search_end = std::chrono::high_resolution_clock::now();

  writer.finalize();
  if (!writer.ok()) {
    return false;
  }

  const double elapsed =
      std::chrono::duration<double>(search_end - search_start).count();
  const int total_queries_executed = target_concurrency;
  const double qps = (elapsed > 0.0 && total_queries_executed > 0)
                         ? total_queries_executed / elapsed
                         : 0.0;

  std::vector<diskann::QueryStats> stats_vec;
  stats_vec.reserve(static_cast<std::size_t>(target_concurrency));
  stats_vec.push_back(primary_stats);
  stats_vec.insert(stats_vec.end(), background_stats.begin(), background_stats.end());
  // log_iteration_stats(L, qps, stats_vec);

  return true;
}

struct ResponseChunk {
  std::size_t offset = 0;
  std::size_t count = 0;
};

std::vector<ResponseChunk> plan_response_chunks(std::size_t total_ids) {
  std::vector<ResponseChunk> chunks;
  if (total_ids == 0) {
    return chunks;
  }

  std::size_t first = total_ids / 2;
  if (first == 0) {
    first = total_ids;
  }
  std::size_t second = total_ids - first;

  chunks.push_back(ResponseChunk{0, first});
  if (second > 0) {
    chunks.push_back(ResponseChunk{first, second});
  }

  return chunks;
}

std::string build_chunk_payload(const std::vector<uint32_t>& result_ids,
                                std::size_t offset, std::size_t count) {
  std::ostringstream payload_stream;
  for (std::size_t i = 0; i < count; ++i) {
    if (i != 0) {
      payload_stream << ',';
    }
    payload_stream << result_ids[offset + i];
  }
  return payload_stream.str();
}

bool send_search_results(int connfd, const QueryExecutionOutput& execution) {
  const std::size_t expected_total =
      static_cast<std::size_t>(execution.request.topk) *
      static_cast<std::size_t>(execution.request.query_count);
  const std::size_t total_ids = execution.result_ids.size();

  if (total_ids != expected_total) {
    std::cout << "[server] warning: result size mismatch expected="
              << expected_total << " actual=" << total_ids << std::endl;
  }

  const auto chunks = plan_response_chunks(total_ids);

  std::ostringstream header_stream;
  header_stream << "RESULT " << chunks.size() << ' ' << total_ids << '\n';
  const std::string header = header_stream.str();
  if (!send_all(connfd, header.data(), header.size())) {
    std::cout << "[server] failed to send result header" << std::endl;
    return false;
  }

  for (std::size_t idx = 0; idx < chunks.size(); ++idx) {
    const auto& chunk = chunks[idx];
    const std::string payload =
        build_chunk_payload(execution.result_ids, chunk.offset, chunk.count);

    std::ostringstream chunk_header_stream;
    chunk_header_stream << "PART " << (idx + 1) << ' ' << chunk.count << ' '
                        << payload.size() << '\n';
    const std::string chunk_header = chunk_header_stream.str();
    if (!send_all(connfd, chunk_header.data(), chunk_header.size())) {
      std::cout << "[server] failed to send chunk header" << std::endl;
      return false;
    }

    if (!payload.empty() &&
        !send_all(connfd, payload.data(), payload.size())) {
      std::cout << "[server] failed to send chunk payload" << std::endl;
      return false;
    }

    if (!send_all(connfd, "\n", 1)) {
      std::cout << "[server] failed to send chunk delimiter" << std::endl;
      return false;
    }

    std::cout << "[server] sent chunk " << (idx + 1) << "/" << chunks.size()
              << " ids=" << chunk.count << std::endl;
  }

  return true;
}

void log_client_ack(int connfd) {
  std::array<char, kBufferLength> ack_buffer{};
  ssize_t ack_bytes = ::recv(connfd, ack_buffer.data(), ack_buffer.size(), 0);
  if (ack_bytes > 0) {
    std::string ack_msg(ack_buffer.data(),
                        static_cast<std::size_t>(ack_bytes));
    std::cout << "[server] client ack: " << sanitize_line(ack_msg)
              << std::endl;
  }
}

void handle_client_connection(int connfd, const AsyncIndexPtr& async_index,
                              diskann::CoroutineScheduler* scheduler) {
  std::cout << "\n[server] connection fd=" << connfd << std::endl;
  SocketCloser conn_guard(connfd);

  try {
    QueryPayload payload;
    if (!receive_query_payload(connfd, payload)) {
      return;
    }

    const bool streaming_requested = (payload.request.query_count == 1);
    const size_t total_ids_expected = static_cast<std::size_t>(payload.request.topk) *
                                      static_cast<std::size_t>(payload.request.query_count);
    auto send_lambda = [connfd](const char* data, size_t len) {
      return send_all(connfd, data, len);
    };
    diskann::streaming::StageChunkWriter stage_writer(send_lambda, 2, total_ids_expected);
    bool streaming_active = streaming_requested;
    if (streaming_active && !stage_writer.begin()) {
      streaming_active = false;
      // std::cout << "[server] streaming header send failed, using buffered response" << std::endl;
    }

    diskann::SearchStreamOptions stream_opts{};
    if (streaming_active) {
      stream_opts.stage_count = 2;
      stream_opts.first_stage_min_results = 0;
      stream_opts.min_ios_before_emit = 2;
      stream_opts.min_steps_before_emit = 2;
      stream_opts.user_context = &stage_writer;
      stream_opts.emit = diskann::streaming::StageChunkWriter::callback_adapter;
      stream_opts.query_id = 0;

      // std::cout << "[server] streaming enabled for single query" << std::endl;
    }

    // print_search_header();
    bool response_ok = true;
    if (streaming_active) {
      response_ok = execute_streaming_query(async_index, scheduler, payload,
                                           stage_writer, stream_opts);
    } else {
      QueryExecutionOutput execution =
          execute_queries(async_index, scheduler, payload);
      response_ok = send_search_results(connfd, execution);
    }

    if (!response_ok) {
      return;
    }

    log_client_ack(connfd);
  } catch (const std::exception& ex) {
    std::cout << "[server] exception: " << ex.what() << std::endl;
  }
}

void run_server(const AsyncIndexPtr& async_index, int listen_socket) {
  diskann::init_scheduler();
  diskann::CoroutineScheduler* scheduler = diskann::get_cor_scheduler();

  omp_set_num_threads(kNumThreads);

  ::signal(SIGINT, stopServerRunning);

  std::cout << "Listening on port " << kDefaultPort << "..." << std::endl;

  while (true) {
    int connfd = ::accept(listen_socket, nullptr, nullptr);
    if (connfd < 0) {
      std::perror("Accept error");
      continue;
    }

    handle_client_connection(connfd, async_index, scheduler);
  }
}

}  // namespace

int main() {
  try {
    diskann::Metric metric = resolve_metric();
    validate_configuration(metric);
    log_async_search_parameters();

    AsyncIndexPtr async_index = load_async_index(metric);
    configure_cache(async_index);

    initialize_background_queries();

    int listen_socket = create_listen_socket(kDefaultPort);
    if (listen_socket < 0) {
      return -1;
    }

    run_server(async_index, listen_socket);
  } catch (const std::exception& ex) {
    std::cerr << "Fatal error: " << ex.what() << std::endl;
    return -1;
  }

  return 0;
}
