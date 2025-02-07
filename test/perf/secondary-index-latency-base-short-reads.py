#!/usr/bin/env python3

import statistics

from os import urandom

from timeit import default_timer as timer
from contextlib import contextmanager
from itertools import repeat, starmap

from tabulate import tabulate

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement


# Taken from https://docs.python.org/3/library/itertools.html#itertools-recipes
def repeatfunc(func, times=None, *args):
    if times is None:
        return starmap(func, repeat(args))
    return starmap(func, repeat(args, times))


def find_avg(func, *args, runs=3):
    times = [val for val in repeatfunc(func, runs, *args)]
    avg_time = round(statistics.mean(times), 2)
    std_dev = round(statistics.stdev(times), 2) if len(times) > 1 else 0
    print(f"Average execution time over {runs} runs: \033[1m{avg_time} +- {std_dev} seconds\033[0m")
    return (avg_time, std_dev)


@contextmanager
def timing_block(label):
    start = end = timer()
    yield lambda : end - start
    end = timer()
    print(f"{label} took {end - start:.2f} seconds")


def hydrate_table(session, partitions: int, partition_size: int):
    print(f"Checking if table ks.t1 is already hydrated...")
    result = session.execute("SELECT * FROM ks.t1 LIMIT 1")
    if (result.current_rows):
        print(f"Table ks.t1 is already hydrated. Skipping...")
        return
    print(f"Populating table ks.t1 with {partitions} partitions, {partition_size} rows each...")
    insert_query = session.prepare("INSERT INTO ks.t1 (pk1, pk2, ck, value) VALUES (?, ?, ?, ?)")
    for pk in range(partitions):
        for ck in range(partition_size):
            session.execute(insert_query, (pk, 1, ck, urandom(1024)))


def run_query(session, fetch_size: int):
    print(f"Querying table ks.t1 with page size = {fetch_size}...")
    stmt = SimpleStatement("SELECT * FROM ks.t1 where pk2 = 1", fetch_size=fetch_size, consistency_level=ConsistencyLevel.QUORUM)
    total_rows = []
    with timing_block("Paged query execution") as t:
        result = session.execute(stmt)
        total_rows.append(len(result.current_rows))
        while result.has_more_pages:
            result = session.execute(stmt, paging_state=result.paging_state)
            total_rows.append(len(result.current_rows))
    print(f"Result pages: {len(total_rows)}\nRows per page: {total_rows}")
    return t()


def print_stats(stats):
    headers = ["Page Size", "Avg Time (sec)", "Std Dev (sec)"]
    print(tabulate(stats, headers=headers, tablefmt="grid"))


def main():
    cluster = Cluster(["127.61.0.1"]) # Use the right IP here.
    session = cluster.connect()
    session.default_timeout = 60

    ## Secondary indexes still don't work with tablets (see 677f9962cf4).
    #session.execute("CREATE KEYSPACE IF NOT EXISTS ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'enabled': true};")
    session.execute("CREATE KEYSPACE IF NOT EXISTS ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'enabled': false};")
    session.execute("CREATE TABLE IF NOT EXISTS ks.t1 (pk1 int, pk2 int, ck int, value blob, PRIMARY KEY ((pk1, pk2), ck));")
    session.execute("CREATE INDEX IF NOT EXISTS ON ks.t1 (pk2);")

    hydrate_table(session, partitions=1000, partition_size=100)

    stats = []
    for fetch_size in [100, 1000, 2000, 5000, 10000, -1]:
        stats.append((fetch_size, *find_avg(run_query, session, fetch_size, runs=5)))
        print("\n-----------------------------\n")

    cluster.shutdown()

    print_stats(stats)


if __name__ == "__main__":
    main()