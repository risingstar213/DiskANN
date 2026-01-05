// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "async_pq_flash_index.h"
#include "coroutine_scheduler.h"
#include "logger.h"
#include "percentile_stats.h"
#include "utils.h"

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
#include <limits>
#include <memory>
#include <optional>
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
    "/mnt/dataset/wiki_dpr_copy/disk_index_wiki_dpr_base_R128_L256_A1.2"};

constexpr uint32_t kBeamWidth = 64;
constexpr uint32_t kVectorDim = 768;
constexpr uint32_t kNumThreads = 1;
constexpr uint32_t kNodesToCache = 0;
constexpr uint32_t kSearchListMultiplier = 40;
constexpr bool kUseReorderData = false;
constexpr uint32_t kIdLength = 8;
constexpr uint32_t kCoroutinesPerThread = 1;

int g_listen_socket = -1;

struct SearchRequest {
  int     topk = 0;
  int     query_count = 0;
  size_t  payload_chars = 0;
};

std::string sanitize_line(const std::string& input) {
  std::string cleaned = input;
  cleaned.erase(std::remove(cleaned.begin(), cleaned.end(), '\r'), cleaned.end());
  cleaned.erase(std::remove(cleaned.begin(), cleaned.end(), '\n'), cleaned.end());
  cleaned.erase(std::remove(cleaned.begin(), cleaned.end(), '\0'), cleaned.end());
  return cleaned;
}

std::optional<SearchRequest> parse_metadata(const std::string& raw_metadata) {
  const std::string metadata = sanitize_line(raw_metadata);
  const auto first_comma = metadata.find(',');
  const auto second_comma = first_comma == std::string::npos
                                 ? std::string::npos
                                 : metadata.find(',', first_comma + 1);

  if (first_comma == std::string::npos || second_comma == std::string::npos) {
    return std::nullopt;
  }

  size_t third_comma = metadata.find(',', second_comma + 1);
  std::string topk_str = metadata.substr(0, first_comma);
  std::string query_count_str =
      metadata.substr(first_comma + 1, second_comma - first_comma - 1);
  std::string payload_str;
  if (third_comma == std::string::npos) {
    payload_str = metadata.substr(second_comma + 1);
  } else {
    payload_str = metadata.substr(second_comma + 1, third_comma - second_comma - 1);
  }

  try {
    SearchRequest request{};
    request.topk = std::stoi(topk_str);
    request.query_count = std::stoi(query_count_str);
    request.payload_chars = std::stoull(payload_str);
    if (request.topk <= 0 || request.query_count <= 0 || request.payload_chars == 0) {
      return std::nullopt;
    }
    return request;
  } catch (const std::exception&) {
    return std::nullopt;
  }
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
                                        diskann::QueryStats* stats) {
  co_await index->async_cached_beam_search(
      query, topk, L, result_ids, result_dists, kBeamWidth, false,
      static_cast<uint32_t>(0), std::numeric_limits<uint32_t>::max(),
      kUseReorderData, stats);
}

}  // namespace

int main() {
  try {
    if (kDataTypeName != "float") {
      std::cerr << "Unsupported data type. Use float!" << std::endl;
      return -1;
    }

    diskann::Metric metric = resolve_metric();

    if (metric == diskann::Metric::INNER_PRODUCT && kDataTypeName != "float") {
      std::cout << "Currently support only floating point data for Inner Product."
                << std::endl;
      return -1;
    }
    if (kUseReorderData && kDataTypeName != "float") {
      std::cout << "Error: Reorder data supported only for float data type."
                << std::endl;
      return -1;
    }

    diskann::cout << "Async Search parameters: #threads: " << kNumThreads
                  << ", ";
    if (kBeamWidth == 0) {
      diskann::cout << "beamwidth to be optimized for each L value" << std::endl;
      return -1;
    } else {
      diskann::cout << " beamwidth: " << kBeamWidth << std::endl;
    }

    std::shared_ptr<AlignedFileReader> reader =
        std::make_shared<AsyncLinuxAlignedFileReader>();
    auto async_index = std::make_shared<diskann::AsyncPQFlashIndex<DataType>>(reader, metric);

    int load_result = async_index->load(kNumThreads, kIndexPathPrefix.data(),
                                        kCoroutinesPerThread);
    if (load_result != 0) {
      return load_result;
    }

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
    node_list.clear();
    node_list.shrink_to_fit();

    diskann::init_scheduler();
    diskann::CoroutineScheduler* scheduler = diskann::get_cor_scheduler();

    omp_set_num_threads(kNumThreads);

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
    server_addr.sin_port = htons(kDefaultPort);

    if (::bind(listen_socket, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
      std::perror("Bind error");
      return -1;
    }

    if (::listen(listen_socket, kMaxPendingConnections) < 0) {
      std::perror("Listen error");
      return -1;
    }

    std::cout << "Listening on port " << kDefaultPort << "..." << std::endl;
  ::signal(SIGINT, stopServerRunning);

    while (true) {
      int connfd = ::accept(listen_socket, nullptr, nullptr);
      if (connfd < 0) {
        std::perror("Accept error");
        continue;
      }
      std::cout << "\n[server] connection fd=" << connfd << std::endl;

      DataType* query = nullptr;
      auto aligned_deleter = [](DataType* ptr) {
        if (ptr != nullptr) {
          diskann::aligned_free(ptr);
        }
      };
      std::unique_ptr<DataType, decltype(aligned_deleter)> query_guard(nullptr, aligned_deleter);

      try {
        std::array<char, kBufferLength> metadata_buffer{};
        ssize_t metadata_bytes =
            ::recv(connfd, metadata_buffer.data(), metadata_buffer.size(), 0);
        if (metadata_bytes <= 0) {
          std::cout << "[server] failed to receive metadata" << std::endl;
          ::close(connfd);
          continue;
        }
        std::string metadata_raw(metadata_buffer.data(),
                                 static_cast<std::size_t>(metadata_bytes));
        auto request_opt = parse_metadata(metadata_raw);
        if (!request_opt.has_value()) {
          std::cout << "[server] metadata parse error: " << metadata_raw << std::endl;
          ::close(connfd);
          continue;
        }
        const SearchRequest request = request_opt.value();
        std::cout << "[server] meta data: topk=" << request.topk
                  << " query_count=" << request.query_count
                  << " payload_chars=" << request.payload_chars << std::endl;

        constexpr std::string_view kAck{"Go"};
        if (!send_all(connfd, kAck.data(), kAck.size())) {
          std::cout << "[server] failed to send ack" << std::endl;
          ::close(connfd);
          continue;
        }

        std::string query_vector_payload;
        if (!recv_exact(connfd, request.payload_chars, query_vector_payload)) {
          std::cout << "[server] failed to receive query payload" << std::endl;
          ::close(connfd);
          continue;
        }
        std::cout << "[server] received query payload size="
                  << query_vector_payload.size() << std::endl;

        size_t query_aligned_dim = 0;
        diskann::load_aligned_bin_mem<DataType>(query_vector_payload, query,
                                                request.query_count,
                                                kVectorDim, query_aligned_dim);
        query_guard.reset(query);

        print_search_header();

        const uint32_t L = static_cast<uint32_t>(request.topk) *
                           kSearchListMultiplier;
        if (L < static_cast<uint32_t>(request.topk)) {
          std::cout << "[server] invalid L computed for topk" << std::endl;
          ::close(connfd);
          continue;
        }

        std::vector<uint64_t> result_ids_64(
            static_cast<std::size_t>(request.topk) * request.query_count);
        std::vector<float> result_dists(
            static_cast<std::size_t>(request.topk) * request.query_count);
        std::vector<diskann::QueryStats> stats(
            static_cast<std::size_t>(request.query_count));

        std::vector<diskann::Task<void>> tasks;
        tasks.reserve(request.query_count);

        auto search_start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < request.query_count; ++i) {
          tasks.emplace_back(async_single_search(
              async_index, query + static_cast<std::size_t>(i) * query_aligned_dim,
              static_cast<uint32_t>(request.topk), L,
              result_ids_64.data() + static_cast<std::size_t>(i) * request.topk,
              result_dists.data() + static_cast<std::size_t>(i) * request.topk,
              stats.data() + i));
        }

        for (auto& task : tasks) {
          if (task.coro) {
            scheduler->schedule_coroutine(task.coro);
          }
        }
        scheduler->run();
        for (auto& task : tasks) {
          task.get_result();
        }
        auto search_end = std::chrono::high_resolution_clock::now();

        const std::chrono::duration<double> diff = search_end - search_start;
        const double qps = request.query_count / diff.count();
        log_iteration_stats(L, qps, stats);

        std::vector<uint32_t> result_ids(result_ids_64.size());
        diskann::convert_types<uint64_t, uint32_t>(
            result_ids_64.data(), result_ids.data(), request.query_count,
            static_cast<uint32_t>(request.topk));

        std::ostringstream response_stream;
        for (std::size_t i = 0; i < result_ids.size(); ++i) {
          if (i != 0) {
            response_stream << ',';
          }
          response_stream << result_ids[i];
        }
        const std::string response = response_stream.str();
        if (!send_all(connfd, response.data(), response.size())) {
          std::cout << "[server] failed to send results" << std::endl;
          ::close(connfd);
          continue;
        }
        std::cout << "[server] sent result ids: " << response << std::endl;

        std::array<char, kBufferLength> ack_buffer{};
        ssize_t ack_bytes = ::recv(connfd, ack_buffer.data(), ack_buffer.size(), 0);
        if (ack_bytes > 0) {
          std::string ack_msg(ack_buffer.data(),
                              static_cast<std::size_t>(ack_bytes));
          std::cout << "[server] client ack: " << sanitize_line(ack_msg)
                    << std::endl;
        }

        query_guard.reset();
        ::close(connfd);
      } catch (const std::exception& ex) {
        std::cout << "[server] exception: " << ex.what() << std::endl;
        ::close(connfd);
      }
    }
  } catch (const std::exception& ex) {
    std::cerr << "Fatal error: " << ex.what() << std::endl;
    return -1;
  }

  return 0;
}
