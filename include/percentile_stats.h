// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cstdint>
#include <functional>
#ifdef _WINDOWS
#include <numeric>
#endif
#include <vector>
#include <algorithm>
#include <cmath>

#ifndef SAMPLE_POINT_NUM
#define SAMPLE_POINT_NUM 10
#endif

#ifndef SAMPLE_POINT_VALUE
#define SAMPLE_POINT_VALUE                                                                                              \
    {                                                                                                                   \
        {0.0, 0.1}, {0.0, 0.2}, {0.0, 0.3}, {0.0, 0.4}, {0.0, 0.5}, {0.0, 0.6}, {0.0, 0.7}, {0.0, 0.8}, {0.0, 0.9},      \
            {0.0, 1.0}                                                                                                  \
    }
#endif

#ifndef HRS_LEN
#define HRS_LEN 24
#endif

#ifndef HRS_VALUE
#define HRS_VALUE                                                                                                       \
    {                                                                                                                   \
        2, 4, 6, 8, 10, 12, 16, 20, 24, 32, 40, 48, 64, 80, 96, 112, 128, 144, 160, 192, 224, 256, 288, 336             \
    }
#endif

#ifndef STEP_TIME
#define STEP_TIME false
#endif

#ifndef STEP_IO
#define STEP_IO true
#endif

#ifndef FULL_RETSET_SWAP
#define FULL_RETSET_SWAP true
#endif

namespace diskann
{
struct QueryStats
{
    float total_us = 0; // total time to process query in micros
    float io_us = 0;    // total time spent in IO
    float cpu_us = 0;   // total time spent in CPU

    unsigned n_4k = 0;         // # of 4kB reads
    unsigned n_8k = 0;         // # of 8kB reads
    unsigned n_12k = 0;        // # of 12kB reads
    unsigned n_ios = 0;        // total # of IOs issued
    unsigned read_size = 0;    // total # of bytes read
    unsigned n_cmps_saved = 0; // # cmps saved
    unsigned n_cmps = 0;       // # cmps
    unsigned n_cache_hits = 0; // # cache_hits
    unsigned n_hops = 0;       // # search hops

    unsigned queries_id = 0;
    unsigned *gold_std = nullptr;
    float *gs_dist = nullptr;
    unsigned dim_gs = 0;
    unsigned recall_at = 0;
    unsigned visited_length = 0;
    unsigned n_steps = 0;
    unsigned hitnum_reset_n[HRS_LEN > 0 ? HRS_LEN : 1] = {0};
    unsigned hitnum_reset_n_final[HRS_LEN > 0 ? HRS_LEN : 1] = {0};
    unsigned time_reset_n[HRS_LEN > 0 ? HRS_LEN : 1] = {0};
    unsigned io_reset_n[HRS_LEN > 0 ? HRS_LEN : 1] = {0};
    unsigned len_frf_n[HRS_LEN > 0 ? HRS_LEN : 1] = {0};
    unsigned hitnum_reset_n_split[HRS_LEN > 0 ? HRS_LEN : 1]
                                   [SAMPLE_POINT_NUM > 0 ? SAMPLE_POINT_NUM : 1] = {{0}};
};

template <typename T>
inline T get_percentile_stats(QueryStats *stats, uint64_t len, float percentile,
                              const std::function<T(const QueryStats &)> &member_fn)
{
    std::vector<T> vals(len);
    for (uint64_t i = 0; i < len; i++)
    {
        vals[i] = member_fn(stats[i]);
    }

    std::sort(vals.begin(), vals.end(), [](const T &left, const T &right) { return left < right; });

    auto retval = vals[(uint64_t)(percentile * len)];
    vals.clear();
    return retval;
}

template <typename T>
inline double get_mean_stats(QueryStats *stats, uint64_t len, const std::function<T(const QueryStats &)> &member_fn)
{
    double avg = 0;
    for (uint64_t i = 0; i < len; i++)
    {
        avg += (double)member_fn(stats[i]);
    }
    return avg / len;
}
} // namespace diskann
