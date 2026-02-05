import duckdb
import json
from enum import Enum
from glob import glob
import argparse

class Source(Enum):
    PARQUET   = 0
    CSV       = 1
    JSON      = 2
    MEMORY    = 3
    FILTERED  = 4
    PROJECTED = 5

def load_data(con, data_dir: str, source: Source):
    tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
    for table in tables:
        if source == Source.PARQUET:
            con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{data_dir}/tpch/{table}.parquet');")
        elif source == Source.CSV:
            con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_csv('{data_dir}/tpch/{table}.csv');")
        elif source == Source.JSON:
            con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_json('{data_dir}/tpch/{table}.json');")
        else:
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM read_parquet('{data_dir}/tpch/{table}.parquet');")
    
    if source == Source.FILTERED or source == Source.PROJECTED:
        for filename in glob(f"sql/tpch/filtered_views/*.sql"):
            with open(filename, "r") as f:
                con.execute(f.read())
    if source == Source.PROJECTED:
        for filename in glob(f"sql/tpch/projected_views/*.sql"):
            with open(filename, "r") as f:
                con.execute(f.read())

def run_benchmark(data_dir: str, source: Source, reps: int, threads: int):
    result = []
    query_dir = "sql/tpch/queries/"
    if source == Source.FILTERED:
        query_dir = "sql/tpch/filtered_queries/"
    if source == Source.PROJECTED:
        query_dir = "sql/tpch/projected_queries/"

    con = duckdb.connect(":memory:")

    con.execute(f"SET threads TO {threads}")
    con.execute("SET enable_object_cache TO false")

    load_data(con, data_dir, source)

    con.execute("PRAGMA enable_profiling='json'")
    
    queries = sorted(glob(query_dir + "*.sql"))
    for filename in queries:
        profile_log = "profile.log"
        con.execute(f"PRAGMA profile_output='{profile_log}'")
        print(filename)
        query_results = []
        for _ in list(range(reps)):
            with open(filename, "r") as f:
                con.execute(f.read()).fetch_arrow_table()

            with open(profile_log, "r") as f:
                profile_data = json.load(f)
                query_results.append(profile_data["cpu_time"])
        result.append(sum(query_results) / len(query_results))

    con.close()

    return result

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="DuckDB with different input configurations.",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-s", "--source",
        type=str,
        default="parquet",
        dest="source",
        metavar="SRC",
        help="Input source (default: parquet)"
    )
    parser.add_argument(
        "-r", "--repetitions",
        type=int,
        default=5,
        dest="reps",
        metavar="REPS",
        help="Number of repetitions per query (default: 5)"
    )
    parser.add_argument(
        "-t", "--threads",
        type=int,
        default=1,
        dest="threads",
        metavar="THREADS",
        help="Number of threads (default: 1)"
    )
    
    args = parser.parse_args()

    data_dir = "data"

    if args.source == "parquet":
        stats = run_benchmark(data_dir, Source.PARQUET, args.reps, args.threads)
    if args.source == "csv":
        stats = run_benchmark(data_dir, Source.CSV, args.reps, args.threads)
    if args.source == "json":
        stats = run_benchmark(data_dir, Source.JSON, args.reps, args.threads)
    if args.source == "memory":
        stats = run_benchmark(data_dir, Source.MEMORY, args.reps, args.threads)
    if args.source == "filtered":
        stats = run_benchmark(data_dir, Source.FILTERED, args.reps, args.threads)
    if args.source == "projected":
        stats = run_benchmark(data_dir, Source.PROJECTED, args.reps, args.threads)

    print(stats)
    #num_queries = len(parquet_times)

    #print(f"{'Query':<5}    {'Parquet (s)':>11}    {'Table (s)':>16}    {'Filtered (s)':>16}")
    #print("-" * 60)

    #memory_speedups = [(1.0 - a / b) * -100.0 for a, b in zip(memory_times, parquet_times)]
    #filtered_speedups = [(1.0 - a / b) * -100.0 for a, b in zip(filtered_times, memory_times)]
    #for i in list(range(num_queries)):
    #    q = f"Q{str(i + 1)}"
    #    print(f"{q:<5}    {parquet_times[i]:>11.4f}    {memory_times[i]:>6.4f} ({memory_speedups[i]:>6.2f}%)    {(filtered_times[i]):>6.4f} ({filtered_speedups[i]:>6.2f}%)")
    
    #print("-" * 60)
    #print(f"Average {sum(memory_speedups)/num_queries:>30.2f}% {sum(filtered_speedups)/num_queries:>18.2f}%")
