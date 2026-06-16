/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <fmt/ranges.h>
#include <bit>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim_all.hpp>

#include <seastar/core/sharded.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/reactor.hh>

#include "locator/tablets.hh"
#include "service/tablet_allocator.hh"
#include "locator/tablet_replication_strategy.hh"
#include "locator/network_topology_strategy.hh"
#include "locator/load_sketch.hh"
#include "test/lib/topology_builder.hh"
#include "replica/tablets.hh"
#include "locator/tablet_replication_strategy.hh"
#include "db/config.hh"
#include "schema/schema_builder.hh"
#include "service/storage_proxy.hh"
#include "db/system_keyspace.hh"
#include "tools/utils.hh"

#include "test/perf/perf.hh"
#include "test/lib/log.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/key_utils.hh"

using namespace locator;
using namespace replica;
using namespace service;
using namespace tools::utils;

namespace bpo = boost::program_options;

static seastar::abort_source aborted;

static const sstring dc = "dc1";

static
cql_test_config tablet_cql_test_config() {
    cql_test_config c;
    return c;
}

static
future<table_id> add_table(cql_test_env& e, sstring test_ks_name = "") {
    auto id = table_id(utils::UUID_gen::get_time_UUID());
    co_await e.create_table([&] (std::string_view ks_name) {
        if (!test_ks_name.empty()) {
            ks_name = test_ks_name;
        }
        return *schema_builder(this_smp_shard_count(), ks_name, id.to_sstring(), id)
                .with_column("p1", utf8_type, column_kind::partition_key)
                .with_column("r1", int32_type)
                .build();
    });
    co_return id;
}

// Run in a seastar thread
static
sstring add_keyspace(cql_test_env& e, std::unordered_map<sstring, int> dc_rf, int initial_tablets = 0) {
    static std::atomic<int> ks_id = 0;
    auto ks_name = fmt::format("keyspace{}", ks_id.fetch_add(1));
    sstring rf_options;
    for (auto& [dc, rf] : dc_rf) {
        rf_options += format(", '{}': {}", dc, rf);
    }
    e.execute_cql(fmt::format("create keyspace {} with replication = {{'class': 'NetworkTopologyStrategy'{}}}"
                              " and tablets = {{'enabled': true, 'initial': {}}}",
                              ks_name, rf_options, initial_tablets)).get();
    return ks_name;
}

static
size_t get_tablet_count(const tablet_metadata& tm) {
    size_t count = 0;
    for (const auto& [table, tmap] : tm.all_tables_ungrouped()) {
        count += std::accumulate(tmap->tablets().begin(), tmap->tablets().end(), size_t(0),
                                 [] (size_t accumulator, const locator::tablet_info& info) {
                                     return accumulator + info.replicas.size();
                                 });
    }
    return count;
}

static
future<> apply_resize_plan(token_metadata& tm, const migration_plan& plan) {
    for (auto [table_id, resize_decision] : plan.resize_plan().resize) {
        co_await tm.tablets().mutate_tablet_map_async(table_id, [&resize_decision] (tablet_map& tmap) {
            resize_decision.sequence_number = tmap.resize_decision().sequence_number + 1;
            tmap.set_resize_decision(resize_decision);
            return make_ready_future();
        });
    }
    for (auto table_id : plan.resize_plan().finalize_resize) {
        auto& old_tmap = tm.tablets().get_tablet_map(table_id);
        testlog.info("Setting new tablet map of size {}", old_tmap.tablet_count() * 2);
        tablet_map tmap(old_tmap.tablet_count() * 2);
        tm.tablets().set_tablet_map(table_id, std::move(tmap));
    }
}

// Reflects the plan in a given token metadata as if the migrations were fully executed.
static
future<> apply_plan(token_metadata& tm, const migration_plan& plan, locator::load_stats& load_stats) {
    for (auto&& mig : plan.migrations()) {
        co_await tm.tablets().mutate_tablet_map_async(mig.tablet.table, [&mig] (tablet_map& tmap) {
            auto tinfo = tmap.get_tablet_info(mig.tablet.tablet);
            tinfo.replicas = replace_replica(tinfo.replicas, mig.src, mig.dst);
            tmap.set_tablet(mig.tablet.tablet, tinfo);
            return make_ready_future();
        });
        // Move tablet size in load_stats to account for the migration
        if (mig.src && mig.dst && mig.src->host != mig.dst->host) {
            auto& tmap = tm.tablets().get_tablet_map(mig.tablet.table);
            const dht::token_range trange = tmap.get_token_range(mig.tablet.tablet);
            lw_shared_ptr<locator::load_stats> new_stats = load_stats.migrate_tablet_size(mig.src->host, mig.dst->host, mig.tablet, trange);

            if (new_stats) {
                load_stats = std::move(*new_stats);
            } else {
                throw std::runtime_error(format("Unable to migrate tablet size in load_stats for migration: {}", mig));
            }
        }
    }
    co_await apply_resize_plan(tm, plan);
}

using seconds_double = std::chrono::duration<double>;

struct rebalance_stats {
    seconds_double elapsed_time = seconds_double(0);
    seconds_double max_rebalance_time = seconds_double(0);
    uint64_t rebalance_count = 0;

    rebalance_stats& operator+=(const rebalance_stats& other) {
        elapsed_time += other.elapsed_time;
        max_rebalance_time = std::max(max_rebalance_time, other.max_rebalance_time);
        rebalance_count += other.rebalance_count;
        return *this;
    }
};

static
rebalance_stats rebalance_tablets(cql_test_env& e, locator::load_stats& load_stats, std::unordered_set<host_id> skiplist = {}) {
    rebalance_stats stats;
    abort_source as;

    auto guard = e.get_raft_group0_client().start_operation(as).get();
    auto& talloc = e.get_tablet_allocator().local();
    auto& stm = e.shared_token_metadata().local();

    // Sanity limit to avoid infinite loops.
    // The x10 factor is arbitrary, it's there to account for more complex schedules than direct migration.
    auto max_iterations = 1 + get_tablet_count(stm.get()->tablets()) * 10;

    for (size_t i = 0; i < max_iterations; ++i) {
        auto prev_lb_stats = *talloc.stats().for_dc(dc);
        auto load_stats_p = make_lw_shared<locator::load_stats>(load_stats);
        auto start_time = std::chrono::steady_clock::now();

        auto plan = talloc.balance_tablets(stm.get(), nullptr, nullptr, load_stats_p, skiplist).get();

        auto end_time = std::chrono::steady_clock::now();
        auto lb_stats = *talloc.stats().for_dc(dc) - prev_lb_stats;

        auto elapsed = std::chrono::duration_cast<seconds_double>(end_time - start_time);
        rebalance_stats iteration_stats = {
            .elapsed_time = elapsed,
            .max_rebalance_time = elapsed,
            .rebalance_count = 1,
        };
        stats += iteration_stats;
        testlog.debug("Rebalance iteration {} took {:.3f} [s]: mig={}, bad={}, first_bad={}, eval={}, skiplist={}, skip: (load={}, rack={}, node={})",
                      i + 1, elapsed.count(),
                      lb_stats.migrations_produced,
                      lb_stats.bad_migrations,
                      lb_stats.bad_first_candidates,
                      lb_stats.candidates_evaluated,
                      lb_stats.migrations_from_skiplist,
                      lb_stats.migrations_skipped,
                      lb_stats.tablets_skipped_rack,
                      lb_stats.tablets_skipped_node);

        if (plan.empty()) {
            // We should not introduce inconsistency between on-disk state and in-memory state
            // as that may violate invariants and cause failures in later operations
            // causing test flakiness.
            save_tablet_metadata(e.local_db(), stm.get()->tablets(), guard.write_timestamp()).get();
            e.get_storage_service().local().update_tablet_metadata({}).get();

            testlog.info("Rebalance took {:.3f} [s] after {} iteration(s)", stats.elapsed_time.count(), i + 1);
            return stats;
        }
        stm.mutate_token_metadata([&] (token_metadata& tm) {
            return apply_plan(tm, plan, load_stats);
        }).get();
    }
    throw std::runtime_error("rebalance_tablets(): convergence not reached within limit");
}

struct params {
    int iterations;
    int nodes;
    std::optional<int> tablets1;
    std::optional<int> tablets2;
    int rf1;
    int rf2;
    int shards;
    int scale1 = 1;
    int scale2 = 1;
    double tablet_size_deviation_factor = 0.5;
};

struct table_balance {
    double shard_overcommit;
    double best_shard_overcommit;
    double node_overcommit;
};

constexpr auto nr_tables = 2;

struct cluster_balance {
    table_balance tables[nr_tables];
};

struct results {
    cluster_balance init;
    cluster_balance worst;
    cluster_balance last;
    rebalance_stats stats;
};

template<>
struct fmt::formatter<table_balance> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const table_balance& b, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{{shard={} (best={}), node={}}}",
                              b.shard_overcommit, b.best_shard_overcommit, b.node_overcommit);
    }
};

template<>
struct fmt::formatter<cluster_balance> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const cluster_balance& r, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{{table1={}, table2={}}}", r.tables[0], r.tables[1]);
    }
};

template<>
struct fmt::formatter<params> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const params& p, FormatContext& ctx) const {
        auto tablets1_per_shard = double(p.tablets1.value_or(0)) * p.rf1 / (p.nodes * p.shards);
        auto tablets2_per_shard = double(p.tablets2.value_or(0)) * p.rf2 / (p.nodes * p.shards);
        return fmt::format_to(ctx.out(), "{{iterations={}, nodes={}, tablets1={} ({:0.1f}/sh), tablets2={} ({:0.1f}/sh), rf1={}, rf2={}, shards={}, tablet_size_deviation_factor={}}}",
                         p.iterations, p.nodes,
                         p.tablets1.value_or(0), tablets1_per_shard,
                         p.tablets2.value_or(0), tablets2_per_shard,
                         p.rf1, p.rf2, p.shards, p.tablet_size_deviation_factor);
    }
};

class tablet_size_generator {
    std::default_random_engine _rnd_engine{std::random_device{}()};
    std::normal_distribution<> _dist;
public:
    explicit tablet_size_generator(double deviation_factor)
        : _dist(default_target_tablet_size, default_target_tablet_size * deviation_factor) {
    }

    uint64_t generate() {
        // We can't have a negative tablet size, which is why we need to minimize it to 0 (with std::max()).
        // One consequence of this is that the average generated tablet size will actually
        // be larger than default_target_tablet_size.
        // This will be especially pronounced as deviation_factor gets larger. For instance:
        //
        // deviation_factor | avg tablet size
        // -----------------+----------------------------------------
        //              1   | default_target_tablet_size * 1.08
        //              1.5 | default_target_tablet_size * 1.22
        //              2   | default_target_tablet_size * 1.39
        //              3   | default_target_tablet_size * 1.76
        return std::max(0.0, _dist(_rnd_engine));
    }
};

void generate_tablet_sizes(double tablet_size_deviation_factor, locator::load_stats& stats, locator::shared_token_metadata& stm) {
    tablet_size_generator tsg(tablet_size_deviation_factor);
    for (auto&& [table, tmap] : stm.get()->tablets().all_tables_ungrouped()) {
        tmap->for_each_tablet([&] (tablet_id tid, const tablet_info& ti) -> future<> {
            for (const auto& replica : ti.replicas) {
                const uint64_t tablet_size = tsg.generate();
                locator::range_based_tablet_id rb_tid {table, tmap->get_token_range(tid)};
                stats.tablet_stats[replica.host].tablet_sizes[rb_tid.table][rb_tid.range] = tablet_size;
                testlog.trace("Generated tablet size {} for {}:{}", tablet_size, table, tid);
            }
            return make_ready_future<>();
        }).get();
    }
}

future<results> test_load_balancing_with_many_tables(params p, bool tablet_aware) {
    auto cfg = tablet_cql_test_config();
    results global_res;
    co_await do_with_cql_env_thread([&] (auto& e) {
        SCYLLA_ASSERT(p.nodes > 0);
        SCYLLA_ASSERT(p.rf1 > 0);
        SCYLLA_ASSERT(p.rf2 > 0);

        const size_t n_hosts = p.nodes;
        const size_t rf1 = p.rf1;
        const size_t rf2 = p.rf2;
        const shard_id shard_count = p.shards;
        const int cycles = p.iterations;
        const uint64_t shard_capacity = default_target_tablet_size * 100;

        struct host_info {
            host_id id;
            endpoint_dc_rack dc_rack;
        };

        topology_builder topo(e);
        std::vector<endpoint_dc_rack> racks;
        std::vector<host_info> hosts;
        locator::load_stats stats;

        auto populate_racks = [&] (const size_t count) {
            SCYLLA_ASSERT(count > 0);
            racks.push_back(topo.rack());
            for (size_t i = 0; i < count - 1; ++i) {
                racks.push_back(topo.start_new_rack());
            }
        };

        const sstring dc1 = topo.dc();
        populate_racks(rf1);

        // The rack for which we output stats
        sstring test_rack = racks.front().rack;

        const size_t rack_count = racks.size();
        std::unordered_map<sstring, uint64_t> rack_capacity;

        auto add_host = [&] (endpoint_dc_rack dc_rack) {
            auto host = topo.add_node(service::node_state::normal, shard_count, dc_rack);
            hosts.emplace_back(host, dc_rack);
            const uint64_t capacity = shard_capacity * shard_count;
            stats.capacity[host] = capacity;
            stats.tablet_stats[host].effective_capacity = capacity;
            rack_capacity[dc_rack.rack] += capacity;
            testlog.info("Added new node: {} / {}:{}", host, dc_rack.dc, dc_rack.rack);
        };

        for (size_t i = 0; i < n_hosts; ++i) {
            add_host(racks[i % rack_count]);
        }

        auto& stm = e.shared_token_metadata().local();

        auto bootstrap = [&] (endpoint_dc_rack dc_rack) {
            add_host(std::move(dc_rack));
            global_res.stats += rebalance_tablets(e, stats);
        };

        auto decommission = [&] (host_id host) {
            const auto it = std::ranges::find_if(hosts, [&] (const host_info& info) {
                return info.id == host;
            });
            if (it == hosts.end()) {
                throw std::runtime_error(format("No such host: {}", host));
            }
            topo.set_node_state(host, service::node_state::decommissioning);
            global_res.stats += rebalance_tablets(e, stats);
            if (stm.get()->tablets().has_replica_on(host)) {
                throw std::runtime_error(format("Host {} still has replicas!", host));
            }
            topo.set_node_state(host, service::node_state::left);
            testlog.info("Node decommissioned: {}", host);
            rack_capacity[it->dc_rack.rack] -= stats.capacity.at(host);
            hosts.erase(it);
            stats.tablet_stats.erase(host);
        };

        auto ks1 = add_keyspace(e, {{dc1, rf1}}, p.tablets1.value_or(1));
        auto ks2 = add_keyspace(e, {{dc1, rf2}}, p.tablets2.value_or(1));
        auto id1 = add_table(e, ks1).get();
        auto id2 = add_table(e, ks2).get();
        schema_ptr s1 = e.local_db().find_schema(id1);
        schema_ptr s2 = e.local_db().find_schema(id2);

        generate_tablet_sizes(p.tablet_size_deviation_factor, stats, stm);

        // Compute table size per rack, and collect all tablets per rack
        std::unordered_map<sstring, std::unordered_map<table_id, uint64_t>> table_sizes_per_rack;
        std::unordered_map<sstring, std::unordered_map<table_id, std::vector<uint64_t>>> tablet_sizes_in_rack;
        for (auto& [host, tls] : stats.tablet_stats) {
            auto host_i = std::ranges::find(hosts, host, &host_info::id);
            if (host_i == hosts.end()) {
                throw std::runtime_error(format("Host {} not found in hosts", host));
            }
            auto rack = host_i->dc_rack.rack;
            for (auto& [table, ranges] : tls.tablet_sizes) {
                for (auto& [trange, tablet_size] : ranges) {
                    table_sizes_per_rack[rack][table] += tablet_size;
                    tablet_sizes_in_rack[rack][table].push_back(tablet_size);
                }
            }
        }

        // Sort the tablet sizes per rack in descending order
        for (auto& [rack, tables] : tablet_sizes_in_rack) {
            for (auto& [table, tablets] : tables) {
                std::ranges::sort(tablets, std::greater<uint64_t>());
            }
        }

        struct node_used_size {
            host_id host;
            uint64_t used = 0;
        };

        // Compute best shard overcommit per table per rack
        std::unordered_map<sstring, std::unordered_map<table_id, double>> best_shard_overcommit_per_rack;
        auto compute_best_overcommit = [&] () {
            auto node_size_compare = [] (const node_used_size& lhs, const node_used_size& rhs) {
                return lhs.used > rhs.used;
            };

            for (auto& all_dc_rack : racks) {
                auto rack = all_dc_rack.rack;

                // Allocate tablet sizes to nodes
                for (auto& [table, tablet_sizes]: tablet_sizes_in_rack.at(rack)) {
                    load_sketch load(e.shared_token_metadata().local().get(), make_lw_shared<locator::load_stats>(stats));

                    // Add nodes to load_sketch and to the nodes_used heap
                    std::vector<node_used_size> nodes_used;
                    for (const auto& [host_id, host_dc_rack] : hosts) {
                        if (rack == host_dc_rack.rack) {
                            load.ensure_node(host_id);
                            nodes_used.push_back({host_id, 0});
                        }
                    }

                    // Allocate tablets to nodes/shards
                    for (uint64_t tablet_size : tablet_sizes) {
                        std::pop_heap(nodes_used.begin(), nodes_used.end(), node_size_compare);
                        host_id add_to_host = nodes_used.back().host;
                        nodes_used.back().used += tablet_size;
                        std::push_heap(nodes_used.begin(), nodes_used.end(), node_size_compare);

                        // Add to the least loaded shard on the least loaded node
                        load.next_shard(add_to_host, 1, tablet_size);
                    }

                    // Get the best overcommit from all the nodes
                    min_max_tracker<locator::disk_usage::load_type> load_minmax;
                    for (const auto& n : nodes_used) {
                        load_minmax.update(load.get_shard_minmax(n.host));
                    }

                    const uint64_t table_size = table_sizes_per_rack.at(rack).at(table);
                    const double ideal_load = double(table_size) / rack_capacity.at(rack);
                    const double best_overcommit = load_minmax.max() / ideal_load;
                    best_shard_overcommit_per_rack[rack][table] = best_overcommit;
                }
            }
        };

        auto check_balance = [&] () -> cluster_balance {
            cluster_balance res;

            testlog.debug("tablet metadata: {}", stm.get()->tablets());

            compute_best_overcommit();

            auto load_stats_p = make_lw_shared<locator::load_stats>(stats);
            int table_index = 0;
            for (auto s : {s1, s2}) {
                auto table = s->id();
                load_sketch load(stm.get(), load_stats_p);
                load.populate(std::nullopt, table).get();

                min_max_tracker<double> shard_overcommit_minmax;
                min_max_tracker<double> node_overcommit_minmax;
                auto rack = test_rack;
                auto table_size = table_sizes_per_rack.at(rack).at(table);
                auto ideal_load = double(table_size) / rack_capacity.at(rack);
                min_max_tracker<double> shard_load_minmax;
                min_max_tracker<double> node_load_minmax;
                for (auto [h, host_dc_rack] : hosts) {
                    if (host_dc_rack.rack != rack) {
                        continue;
                    }
                    auto minmax = load.get_shard_minmax(h);
                    auto node_load = load.get_load(h);
                    auto overcommit = double(minmax.max()) / ideal_load;
                    testlog.info("Load on host {} for table {}: total={}, min={}, max={}, spread={}, ideal={}, overcommit={}",
                                h, s->cf_name(), node_load, minmax.min(), minmax.max(), minmax.max() - minmax.min(), ideal_load, overcommit);
                    node_load_minmax.update(node_load);
                    shard_load_minmax.update(minmax.max());
                }

                auto shard_overcommit = shard_load_minmax.max() / ideal_load;
                auto best_shard_overcommit = best_shard_overcommit_per_rack.at(rack).at(table);
                testlog.info("Shard overcommit: {} best: {}", shard_overcommit, best_shard_overcommit);

                auto node_imbalance = node_load_minmax.max() - node_load_minmax.min();
                auto node_overcommit = node_load_minmax.max() / ideal_load;
                testlog.info("Node imbalance in min={}, max={}, spread={}, ideal={}, overcommit={}",
                            node_load_minmax.min(), node_load_minmax.max(), node_imbalance, ideal_load, node_overcommit);

                shard_overcommit_minmax.update(shard_overcommit);
                node_overcommit_minmax.update(node_overcommit);

                res.tables[table_index++] = {
                    .shard_overcommit = shard_overcommit_minmax.max(),
                    .best_shard_overcommit = best_shard_overcommit,
                    .node_overcommit = node_overcommit_minmax.max(),
                };
            }

            for (int i = 0; i < nr_tables; i++) {
                auto t = res.tables[i];
                global_res.worst.tables[i].shard_overcommit = std::max(global_res.worst.tables[i].shard_overcommit, t.shard_overcommit);
                global_res.worst.tables[i].node_overcommit = std::max(global_res.worst.tables[i].node_overcommit, t.node_overcommit);
            }

            testlog.info("Overcommit: {}", res);
            return res;
        };

        testlog.debug("tablet metadata: {}", stm.get()->tablets());

        e.get_tablet_allocator().local().set_use_table_aware_balancing(tablet_aware);

        check_balance();

        rebalance_tablets(e, stats);

        global_res.init = global_res.worst = check_balance();

        for (int i = 0; i < cycles; i++) {
            const auto [id, dc_rack] = hosts[0];

            bootstrap(dc_rack);
            check_balance();

            decommission(id);
            global_res.last = check_balance();
        }
    }, cfg);
    co_return global_res;
}

void test_parallel_scaleout(const bpo::variables_map& opts) {
    const shard_id shard_count = opts["shards"].as<int>();
    const int nr_tables = opts["tables"].as<int>();
    const int tablets_per_table = opts["tablets_per_table"].as<int>();
    const int nr_racks = opts["racks"].as<int>();
    const int initial_nodes = nr_racks * opts["nodes-per-rack"].as<int>();
    const int extra_nodes = nr_racks * opts["extra-nodes-per-rack"].as<int>();
    const double tablet_size_deviation_factor = opts["tablet-size-deviation-factor"].as<double>();

    auto cfg = tablet_cql_test_config();
    cfg.db_config->rf_rack_valid_keyspaces(true);
    results global_res;
    do_with_cql_env_thread([&] (auto& e) {
        topology_builder topo(e);
        locator::load_stats stats;

        std::vector<endpoint_dc_rack> racks;
        racks.push_back(topo.rack());
        for (int i = 1; i < nr_racks; ++i) {
            racks.push_back(topo.start_new_rack());
        }

        auto add_host = [&] (endpoint_dc_rack rack) {
            auto host = topo.add_node(service::node_state::normal, shard_count, rack);
            const uint64_t capacity = default_target_tablet_size * shard_count * 100;
            stats.capacity[host] = capacity;
            stats.tablet_stats[host].effective_capacity = capacity;
            testlog.info("Added new node: {}", host);
        };

        auto add_hosts = [&] (int n) {
            for (int i = 0; i < n; ++i) {
                add_host(racks[i % racks.size()]);
            }
        };

        add_hosts(initial_nodes);

        testlog.info("Creating schema");
        auto ks1 = add_keyspace(e, {{topo.dc(), nr_racks}}, tablets_per_table);
        seastar::parallel_for_each(std::views::iota(0, nr_tables), [&] (int) -> future<> {
            return add_table(e, ks1).discard_result();
        }).get();

        generate_tablet_sizes(tablet_size_deviation_factor, stats, e.shared_token_metadata().local());

        testlog.info("Initial rebalancing");
        rebalance_tablets(e, stats);

        testlog.info("Scaleout");
        add_hosts(extra_nodes);
        global_res.stats += rebalance_tablets(e, stats);
    }, cfg).get();
}

future<> run_simulation(const params& p, const sstring& name = "") {
    testlog.info("[run {}] params: {}", name, p);

    auto total_tablet_count = p.tablets1.value_or(0) * p.rf1 + p.tablets2.value_or(0) * p.rf2;
    testlog.info("[run {}] tablet count: {}", name, total_tablet_count);
    testlog.info("[run {}] tablet count / shard: {:.3f}", name, double(total_tablet_count) / (p.nodes * p.shards));

    auto res = co_await test_load_balancing_with_many_tables(p, true);
    testlog.info("[run {}] Overcommit       : init : {}", name, res.init);
    testlog.info("[run {}] Overcommit       : worst: {}", name, res.worst);
    testlog.info("[run {}] Overcommit       : last : {}", name, res.last);
    testlog.info("[run {}] Overcommit       : time : {:.3f} [s], max={:.3f} [s], count={}", name,
                 res.stats.elapsed_time.count(), res.stats.max_rebalance_time.count(), res.stats.rebalance_count);

    if (res.stats.elapsed_time > seconds_double(1)) {
        testlog.warn("[run {}] Scheduling took longer than 1s!", name);
    }

    auto old_res = co_await test_load_balancing_with_many_tables(p, false);
    testlog.info("[run {}] Overcommit (old) : init : {}", name, old_res.init);
    testlog.info("[run {}] Overcommit (old) : worst: {}", name, old_res.worst);
    testlog.info("[run {}] Overcommit (old) : last : {}", name, old_res.last);
    testlog.info("[run {}] Overcommit       : time : {:.3f} [s], max={:.3f} [s], count={}", name,
                 old_res.stats.elapsed_time.count(), old_res.stats.max_rebalance_time.count(), old_res.stats.rebalance_count);

    for (int i = 0; i < nr_tables; ++i) {
        if (res.worst.tables[i].shard_overcommit > old_res.worst.tables[i].shard_overcommit) {
            testlog.warn("[run {}] table{} shard overcommit worse!", name, i + 1);
        }
        auto overcommit = res.worst.tables[i].shard_overcommit;
        if (overcommit > 1.2) {
            testlog.warn("[run {}] table{} shard overcommit {:.4f} > 1.2!", name, i + 1, overcommit);
        }
    }
}

future<> run_simulations(const boost::program_options::variables_map& app_cfg) {
    for (auto i = 0; i < app_cfg["runs"].as<int>(); i++) {
        constexpr int MIN_RF = 1;
        constexpr int MAX_RF = 3;

        auto shards = 1 << tests::random::get_int(0, 8);
        auto rf1 = tests::random::get_int(MIN_RF, MAX_RF);
        // FIXME: Once we allow for RF <= #racks (and not just RF == #racks), we can randomize this RF too.
        // For now, the values must be equal.
        auto rf2 = rf1;
        auto scale1 = 1 << tests::random::get_int(0, 5);
        auto scale2 = 1 << tests::random::get_int(0, 5);
        auto nodes = tests::random::get_int(rf1 + rf2, 2 *  MAX_RF);
        // results in a deviation factor of 0.0 - 2.0
        auto tablet_size_deviation_factor = tests::random::get_int(0, 200) / 100.0;

        params p {
            .iterations = app_cfg["iterations"].as<int>(),
            .nodes = nodes,
            .tablets1 = std::bit_ceil<size_t>(div_ceil(shards * nodes, rf1) * scale1),
            .tablets2 = std::bit_ceil<size_t>(div_ceil(shards * nodes, rf2) * scale2),
            .rf1 = rf1,
            .rf2 = rf2,
            .shards = shards,
            .scale1 = scale1,
            .scale2 = scale2,
            .tablet_size_deviation_factor = tablet_size_deviation_factor
        };

        auto name = format("#{}", i);
        co_await run_simulation(p, name);
    }
}

namespace perf {

void run_add_dec(const bpo::variables_map& opts) {
    if (opts.contains("runs")) {
        run_simulations(opts).get();
    } else {
        params p {
            .iterations = opts["iterations"].as<int>(),
            .nodes = opts["nodes"].as<int>(),
            .tablets1 = opts["tablets1"].as<int>(),
            .tablets2 = opts["tablets2"].as<int>(),
            .rf1 = opts["rf1"].as<int>(),
            .rf2 = opts["rf2"].as<int>(),
            .shards = opts["shards"].as<int>(),
            .tablet_size_deviation_factor = opts["tablet-size-deviation-factor"].as<double>(),
        };
        run_simulation(p).get();
    }
}

void show_migration_target_pow2s(const bpo::variables_map& opts) {
    const int nr_racks = opts["racks"].as<int>();
    const int nodes_per_rack = opts["nodes-per-rack"].as<int>();
    const shard_id shards_per_node = static_cast<shard_id>(opts["shards"].as<int>());
    const int rf = opts["rf"].as<int>();

    // Parse table sizes from --logical-table-sizes-gb or --logical-table-size-gb.
    // The former is a comma-separated list of sizes, the latter is a single common
    // size for all tables.
    std::vector<uint64_t> table_sizes;
    const auto sizes_str = opts["logical-table-sizes-gb"].as<std::string>();
    if (!sizes_str.empty()) {
        std::vector<sstring> tokens;
        boost::split(tokens, sizes_str, boost::is_any_of(","));
        table_sizes.reserve(tokens.size());
        for (auto& token : tokens) {
            boost::trim_all(token);
            table_sizes.push_back(uint64_t(std::stoul(token)) << 30);
        }
    } else {
        table_sizes.assign(opts["tables"].as<int>(), uint64_t(opts["logical-table-size-gb"].as<int>()) << 30);
    }
    const int nr_tables = static_cast<int>(table_sizes.size());

    auto cfg = tablet_cql_test_config();
    if (opts.count("tablets-initial-scale-factor")) {
        cfg.db_config->tablets_initial_scale_factor(opts["tablets-initial-scale-factor"].as<double>());
    }
    if (opts.count("tablets-per-shard-goal")) {
        cfg.db_config->tablets_per_shard_goal(static_cast<unsigned>(opts["tablets-per-shard-goal"].as<int>()));
    }
    if (opts.count("target-tablet-size-in-bytes")) {
        cfg.db_config->target_tablet_size_in_bytes(static_cast<uint64_t>(opts["target-tablet-size-in-bytes"].as<int>()));
    }

    do_with_cql_env_thread([&] (auto& e) {
        // Build topology.
        topology_builder topo(e);
        for (int r = 0; r < nr_racks; ++r) {
            if (r > 0) {
                topo.start_new_rack();
            }
            for (int n = 0; n < nodes_per_rack; ++n) {
                topo.add_node(service::node_state::normal, shards_per_node);
            }
        }

        // Create vnode-based keyspace and tables.
        const auto ks_name = sstring("ks_vnodes");
        e.execute_cql(fmt::format(
                "create keyspace {} with replication = {{'class': 'NetworkTopologyStrategy', '{}': {}}}"
                " and tablets = {{'enabled': false}}",
                ks_name, topo.dc(), rf)).get();

        std::vector<table_id> table_ids;
        table_ids.reserve(nr_tables);
        for (int t = 0; t < nr_tables; ++t) {
            const auto tname = fmt::format("t{}", t);
            e.execute_cql(fmt::format("create table {}.{} (pk int primary key)", ks_name, tname)).get();
            table_ids.push_back(e.local_db().find_schema(ks_name, tname)->id());
        }

        // Get the tablet-aware replication strategy from the vnode-based keyspace.
        auto& ks = e.local_db().find_keyspace(ks_name);
        auto* trs = dynamic_cast<const locator::tablet_aware_replication_strategy*>(&ks.get_replication_strategy());
        if (!trs) {
            throw std::runtime_error("Keyspace does not have a tablet-aware replication strategy");
        }

        // Compute target pow2s for vnodes-to-tablets migration.
        service::size_per_table_map sizes;
        for (int i = 0; i < nr_tables; ++i) {
            sizes[table_ids[i]] = table_sizes[i];
        }
        auto target_pow2s = e.get_tablet_allocator().local().compute_migration_target_pow2s(trs, sizes).get();

        // Print results.
        const int total_nodes = nr_racks * nodes_per_rack;
        const int total_shards = total_nodes * static_cast<int>(shards_per_node);

        fmt::print("\n");
        fmt::print("Topology:  {} rack(s) x {} node(s)/rack x {} shard(s)/node, RF={}\n",
                   nr_racks, nodes_per_rack, shards_per_node, rf);
        fmt::print("Config:    tablets_initial_scale_factor={}, tablets_per_shard_goal={}\n",
                   cfg.db_config->tablets_initial_scale_factor(),
                   cfg.db_config->tablets_per_shard_goal());
        fmt::print("Tables:    {}\n", nr_tables);
        fmt::print("\n");

        // Pre-format all row values to compute the max width per column.
        struct row {
            sstring table;
            sstring size_gb;
            sstring target_pow2;
            sstring avg_tablet_size_gb;
            sstring replicas_per_shard;
        };

        std::vector<row> rows;
        rows.reserve(nr_tables);
        double aggregate_replicas_per_shard = 0;
        for (int i = 0; i < nr_tables; ++i) {
            const uint64_t size = table_sizes[i];
            const auto it = target_pow2s.find(table_ids[i]);
            if (it == target_pow2s.end()) {
                rows.push_back({fmt::format("t{}", i), fmt::format("{}", size >> 30), "N/A", "N/A", "N/A"});
                continue;
            }
            const size_t pow2 = it->second;
            const double replicas_per_shard = double(pow2 * rf) / total_shards;
            const double avg_tablet_size = double(size) / pow2 / (1u << 30);
            aggregate_replicas_per_shard += replicas_per_shard;
            rows.push_back({
                fmt::format("t{}", i),
                fmt::format("{}", size >> 30),
                fmt::format("{}", pow2),
                fmt::format("{:.2f}", avg_tablet_size),
                fmt::format("{:.2f}", replicas_per_shard),
            });
        }

        // Column headers.
        const sstring H_TABLE = "table";
        const sstring H_SIZE = "size (GiB)";
        const sstring H_POW2 = "target pow2";
        const sstring H_AVG = "avg tablet size (GiB)";
        const sstring H_RPS = "replicas/shard";

        // Compute column widths: max(header width, max data width).
        auto w_t = H_TABLE.size();
        auto w_size = H_SIZE.size();
        auto w_pow2 = H_POW2.size();
        auto w_avg = H_AVG.size();
        auto w_rps = H_RPS.size();
        for (const auto& r : rows) {
            w_t = std::max(w_t, r.table.size());
            w_size = std::max(w_size, r.size_gb.size());
            w_pow2 = std::max(w_pow2, r.target_pow2.size());
            w_avg = std::max(w_avg, r.avg_tablet_size_gb.size());
            w_rps = std::max(w_rps, r.replicas_per_shard.size());
        }

        // Print per-table results as an aligned table.
        fmt::print("  Per-table results:\n\n");
        fmt::print("    {:<{}}   {:>{}}   {:>{}}   {:>{}}   {:>{}}\n",
                H_TABLE, w_t, H_SIZE, w_size, H_POW2, w_pow2, H_AVG, w_avg, H_RPS, w_rps);
        fmt::print("    {:-<{}}   {:-<{}}   {:-<{}}   {:-<{}}   {:-<{}}\n",
                "", w_t, "", w_size, "", w_pow2, "", w_avg, "", w_rps);
        for (const auto& row : rows) {
            fmt::print("    {:<{}}   {:>{}}   {:>{}}   {:>{}}   {:>{}}\n",
                    row.table, w_t, row.size_gb, w_size, row.target_pow2, w_pow2,
                    row.avg_tablet_size_gb, w_avg, row.replicas_per_shard, w_rps);
        }
        fmt::print("\n");
        fmt::print("  Aggregate tablet replicas per shard:  {:.2f}\n", aggregate_replicas_per_shard);
        fmt::print("\n");
    }, cfg).get();
}

using operation_func = std::function<void(const bpo::variables_map&)>;

const std::vector<operation_option> global_options {};
const std::vector<operation_option> global_positional_options{};

const std::map<operation, operation_func> operations_with_func{
    {
        {{"rolling-add-dec",
         "Sequence of bootstraps and decommissions with two tables",
         "",
         {
            typed_option<int>("runs", "Number of simulation runs."),
            typed_option<int>("iterations", 8, "Number of topology-changing cycles in each run."),
            typed_option<int>("tablets1", 512, "Number of tablets for the first table."),
            typed_option<int>("tablets2", 128, "Number of tablets for the second table."),
            typed_option<int>("rf1", 1, "Replication factor for the first table."),
            typed_option<int>("rf2", 1, "Replication factor for the second table."),
            typed_option<int>("nodes", 3, "Number of nodes in the cluster."),
            typed_option<int>("shards", 30, "Number of shards per node."),
            typed_option<double>("tablet-size-deviation-factor", 0.5, "Deviation factor for the tablet size random generator.")
          }
        }, &run_add_dec},

        {{"parallel-scaleout",
         "Simulates a single scale-out involving simultaneous addition of multiple nodes per rack",
         "",
         {
            typed_option<int>("tablets_per_table", 256, "Number of tablets per table."),
            typed_option<int>("tables", 70, "Table count."),
            typed_option<int>("nodes-per-rack", 5, "Number of initial nodes per rack."),
            typed_option<int>("extra-nodes-per-rack", 3, "Number of nodes to add per rack."),
            typed_option<int>("racks", 2, "Number of racks."),
            typed_option<int>("shards", 88, "Number of shards per node."),
            typed_option<double>("tablet-size-deviation-factor", 0.5, "Deviation factor for the tablet size random generator.")
          }
        }, &test_parallel_scaleout},

        {{"migration-target-pow2s",
         "Show target pow2 tablet counts for a list of tables migrating from vnodes to tablets",
         "",
         {
            typed_option<int>("racks", 3, "Number of racks in the DC."),
            typed_option<int>("nodes-per-rack", 3, "Nodes per rack."),
            typed_option<int>("shards", 8, "Shards per node."),
            typed_option<int>("rf", 3, "Replication factor (single DC, numeric)."),
            typed_option<int>("tables", 1, "Number of migrating tables."),
            typed_option<int>("logical-table-size-gb", 0, "Size of unique data per table in GiB (0 = empty)."),
            typed_option<std::string>("logical-table-sizes-gb", "", "Comma-separated list of logical table sizes in GiB. When provided, overrides --tables and --logical-table-size-gb."),
            typed_option<double>("tablets-initial-scale-factor", "tablets_initial_scale_factor (default: from scylla config)."),
            typed_option<int>("target-tablet-size-in-bytes", "target_tablet_size_in_bytes (default: from scylla config)."),
            typed_option<int>("tablets-per-shard-goal",  "tablets_per_shard_goal (default: from scylla config)."),
          }
        }, &show_migration_target_pow2s},
    }
};

int scylla_tablet_load_balancing_main(int argc, char** argv) {
    const auto operations = operations_with_func | std::views::keys | std::ranges::to<std::vector>();
    tool_app_template::config app_cfg{
            .name = "perf-load-balancing",
            .description = "Tests tablet load balancer in various scenarios",
            .logger_name = testlog.name(),
            .lsa_segment_pool_backend_size_mb = 100,
            .operations = std::move(operations),
            .global_options = &global_options,
            .global_positional_options = &global_positional_options,
            .db_cfg_ext = db_config_and_extensions()
    };
    tool_app_template app(std::move(app_cfg));

    return app.run_async(argc, argv, [] (const operation& operation, const bpo::variables_map& app_config) {
        auto stop_test = defer([] {
            aborted.request_abort();
        });
        try {
            operations_with_func.at(operation)(app_config);
            return 0;
        } catch (seastar::abort_requested_exception&) {
            // Ignore
        }
        return 1;
    });
}

} // namespace perf
