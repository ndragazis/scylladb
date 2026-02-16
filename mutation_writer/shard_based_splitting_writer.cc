/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "mutation_writer/shard_based_splitting_writer.hh"

#include <seastar/core/shared_mutex.hh>
#include <seastar/core/on_internal_error.hh>

#include "dht/i_partitioner.hh"
#include "mutation_writer/feed_writers.hh"
#include "dht/token.hh"
#include "seastar/coroutine/parallel_for_each.hh"

static logging::logger slogger("shard_based_splitting_mutation_writer");

namespace mutation_writer {

class shard_based_splitting_mutation_writer {
    using shard_writer = bucket_writer;

private:
    schema_ptr _schema;
    reader_permit _permit;
    mutation_reader_consumer _consumer;
    owned_ranges_ptr _owned_ranges;
    unsigned _current_shard;
    std::vector<std::optional<shard_writer>> _shards;
    std::optional<dht::token_range_vector::const_iterator> _current_token_range;

    future<> write_to_shard(mutation_fragment_v2&& mf) {
        auto& writer = *_shards[_current_shard];
        return writer.consume(std::move(mf));
    }
public:
    shard_based_splitting_mutation_writer(schema_ptr schema, reader_permit permit, mutation_reader_consumer consumer, owned_ranges_ptr owned_ranges)
        : _schema(std::move(schema))
        , _permit(std::move(permit))
        , _consumer(std::move(consumer))
        , _owned_ranges(std::move(owned_ranges))
        , _shards(_owned_ranges ? 1 : smp::count)
    {
        if (_owned_ranges && _owned_ranges->empty()) {
            on_internal_error(slogger, "Owned ranges cannot be empty");
        }
    }

    future<> consume(partition_start&& ps) {
        if (_owned_ranges) {
            auto token = ps.key().token();
            bool advance = false;
            if (_current_token_range) {
                auto& token_range = *_current_token_range.value();
                if ((advance = token_range.after(token, dht::token_comparator()))) {
                    slogger.info("Token {} is after current range {}: advancing to the next range", token, token_range);
                }
            } else {
                advance = true;
            }
            if (advance) [[unlikely]] {
                // Use a single shard per range for vnodes resharding
                _current_shard = 0;
                if (auto& shard_writer = _shards[_current_shard]) {
                    shard_writer->consume_end_of_stream();
                    co_await shard_writer->close();
                    shard_writer.reset();
                }
                do {
                    if (_current_token_range) {
                        ++*_current_token_range;
                    } else {
                        _current_token_range.emplace(_owned_ranges->begin());
                    }
                    if (_current_token_range == _owned_ranges->end()) {
                        on_internal_error(slogger, format("Token {} is outside of owned ranges", token));
                    }
                } while (!(*_current_token_range)->contains(token, dht::token_comparator()));
            }
        } else {
            _current_shard = dht::static_shard_of(*_schema, ps.key().token()); // FIXME: Use table sharder
        }
        if (!_shards[_current_shard]) {
            _shards[_current_shard] = shard_writer(_schema, _permit, _consumer);
        }
        co_await write_to_shard(mutation_fragment_v2(*_schema, _permit, std::move(ps)));
    }

    future<> consume(static_row&& sr) {
        return write_to_shard(mutation_fragment_v2(*_schema, _permit, std::move(sr)));
    }

    future<> consume(clustering_row&& cr) {
        return write_to_shard(mutation_fragment_v2(*_schema, _permit, std::move(cr)));
    }

    future<> consume(range_tombstone_change&& rt) {
        return write_to_shard(mutation_fragment_v2(*_schema, _permit, std::move(rt)));
    }

    future<> consume(partition_end&& pe) {
        return write_to_shard(mutation_fragment_v2(*_schema, _permit, std::move(pe)));
    }

    void consume_end_of_stream() {
        for (auto& shard : _shards) {
            if (shard) {
                shard->consume_end_of_stream();
            }
        }
    }
    void abort(std::exception_ptr ep) {
        for (auto&& shard : _shards) {
            if (shard) {
                shard->abort(ep);
            }
        }
    }
    future<> close() noexcept {
        return parallel_for_each(_shards, [] (std::optional<shard_writer>& shard) {
            return shard ? shard->close() : make_ready_future<>();
        });
    }
};

future<> segregate_by_shard(mutation_reader producer, mutation_reader_consumer consumer, compaction::owned_ranges_ptr owned_ranges) {
    auto schema = producer.schema();
    auto permit = producer.permit();
    return feed_writer(
        std::move(producer),
        shard_based_splitting_mutation_writer(std::move(schema), std::move(permit), std::move(consumer), std::move(owned_ranges)));
}
} // namespace mutation_writer
