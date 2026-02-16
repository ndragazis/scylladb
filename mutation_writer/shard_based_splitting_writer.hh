/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "readers/mutation_reader.hh"
#include "compaction/compaction_fwd.hh"

namespace mutation_writer {

using owned_ranges_ptr = compaction::owned_ranges_ptr;

// Given a producer that may contain data for all shards, consume it in a per-shard
// manner. This is useful, for instance, in the resharding process where a user changes
// the amount of CPU assigned to Scylla and we have to rewrite the SSTables to their new
// owners.
future<> segregate_by_shard(mutation_reader producer, mutation_reader_consumer consumer, owned_ranges_ptr owned_ranges);

} // namespace mutation_writer
