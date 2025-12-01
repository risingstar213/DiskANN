// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "percentile_stats.h"

namespace diskann
{
namespace streaming
{

class StageChunkWriter
{
  public:
    using SendFn = std::function<bool(const char *, size_t)>;

    StageChunkWriter(SendFn send_fn, uint32_t total_parts, size_t total_ids)
        : _send_fn(std::move(send_fn)), _total_parts(total_parts), _total_ids_expected(total_ids)
    {
    }

    bool begin()
    {
        return ensure_header();
    }

    bool emit(uint32_t query_id, uint32_t stage_idx, const uint32_t *ids, size_t count, bool is_final_stage)
    {
        if (_has_error)
        {
            return false;
        }

        if (ids == nullptr && count > 0)
        {
            _has_error = true;
            return false;
        }

        if (!ensure_header())
        {
            _has_error = true;
            return false;
        }

        std::vector<uint32_t> payload;
        if (ids != nullptr && count > 0)
        {
            payload.reserve(count);
        }

        if (stage_idx == 0 && !is_final_stage)
        {
            auto &cache = _stage_one_cache[query_id];
            cache.clear();
            for (size_t i = 0; i < count; ++i)
            {
                uint32_t id = ids[i];
                payload.push_back(id);
                cache.insert(id);
            }
        }
        else
        {
            payload.reserve(count);
            auto cache_it = _stage_one_cache.find(query_id);
            if (cache_it == _stage_one_cache.end())
            {
                for (size_t i = 0; i < count; ++i)
                {
                    payload.push_back(ids[i]);
                }
            }
            else
            {
                const auto &cache = cache_it->second;
                for (size_t i = 0; i < count; ++i)
                {
                    uint32_t id = ids[i];
                    if (cache.find(id) == cache.end())
                    {
                        payload.push_back(id);
                    }
                }
                if (is_final_stage)
                {
                    _stage_one_cache.erase(cache_it);
                }
            }
        }

        const bool should_send_empty_chunk = is_final_stage && payload.empty();
        if (payload.empty() && !should_send_empty_chunk)
        {
            return true;
        }

        if (!send_chunk(payload))
        {
            _has_error = true;
            return false;
        }

        _sent_ids += payload.size();
        return true;
    }

    void finalize()
    {
        if (_has_error)
        {
            return;
        }

        while (_chunks_sent < _total_parts)
        {
            std::vector<uint32_t> empty_payload;
            if (!send_chunk(empty_payload))
            {
                _has_error = true;
                break;
            }
        }
    }

    bool ok() const
    {
        return !_has_error;
    }

    static void callback_adapter(void *ctx, uint32_t query_id, uint32_t stage_idx, const uint32_t *ids,
                                 const float *, size_t count, bool is_final_stage)
    {
        auto *writer = reinterpret_cast<StageChunkWriter *>(ctx);
        if (writer == nullptr || writer->_has_error)
        {
            return;
        }

        // std::cout << "Emitting stage " << stage_idx << " for query " << query_id
        //           << " with " << count << " ids: ";

        // // put ids
        // for (size_t i = 0; i < count; ++i)
        // {
        //     if (i != 0)
        //     {
        //         std::cout << ",";
        //     }
        //     std::cout << ids[i];
        // }

        // std::cout << (is_final_stage ? " (final stage)" : "") << std::endl;

        if (!writer->emit(query_id, stage_idx, ids, count, is_final_stage))
        {
            writer->_has_error = true;
        }
    }

  private:
    bool ensure_header()
    {
        if (_header_sent)
        {
            return true;
        }
        std::ostringstream oss;
        oss << "RESULT " << _total_parts << ' ' << _total_ids_expected << '\n';
        const std::string header = oss.str();
        if (!_send_fn(header.c_str(), header.size()))
        {
            return false;
        }
        _header_sent = true;
        return true;
    }

    bool send_chunk(const std::vector<uint32_t> &payload)
    {
        if (!ensure_header())
        {
            return false;
        }
        if (_chunks_sent >= _total_parts)
        {
            return true;
        }

        const uint32_t chunk_idx = _chunks_sent + 1;
        std::string payload_body = build_payload(payload);
        std::ostringstream header;
        header << "PART " << chunk_idx << ' ' << payload.size() << ' ' << payload_body.size() << '\n';
        const std::string header_str = header.str();
        if (!_send_fn(header_str.c_str(), header_str.size()))
        {
            return false;
        }

        if (!payload_body.empty())
        {
            if (!_send_fn(payload_body.c_str(), payload_body.size()))
            {
                return false;
            }
        }

        if (!_send_fn("\n", 1))
        {
            return false;
        }

        _chunks_sent++;
        return true;
    }

    static std::string build_payload(const std::vector<uint32_t> &payload)
    {
        if (payload.empty())
        {
            return {};
        }
        std::ostringstream oss;
        for (size_t i = 0; i < payload.size(); ++i)
        {
            if (i != 0)
            {
                oss << ',';
            }
            oss << payload[i];
        }
        return oss.str();
    }

    SendFn _send_fn;
    uint32_t _total_parts;
    size_t _total_ids_expected;
    uint32_t _chunks_sent = 0;
    size_t _sent_ids = 0;
    bool _header_sent = false;
    bool _has_error = false;
    std::unordered_map<uint32_t, std::unordered_set<uint32_t>> _stage_one_cache;
};

class QueryStatsActivator
{
    public:
        QueryStatsActivator() = default;

        void configure(QueryStats *stats, size_t count, size_t per_query_dim, size_t base_query_id = 0)
        {
                if (stats == nullptr || count == 0)
                {
                        return;
                }

                size_t safe_dim = std::max<size_t>(1, per_query_dim);
                _gold_storage.assign(count * safe_dim, 0u);

                for (size_t idx = 0; idx < count; ++idx)
                {
                        auto &entry = stats[idx];
                        entry.gold_std = _gold_storage.data();
                        entry.dim_gs = static_cast<unsigned>(safe_dim);
                        entry.recall_at = static_cast<unsigned>(safe_dim);
                        entry.queries_id = static_cast<unsigned>(base_query_id + idx);
                        entry.gs_dist = nullptr;
                }
        }

    private:
        std::vector<unsigned> _gold_storage;
};

} // namespace streaming
} // namespace diskann