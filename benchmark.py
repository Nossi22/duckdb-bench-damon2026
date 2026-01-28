import duckdb
import json
from enum import Enum
from glob import glob

class Source(Enum):
    PARQUET   = 0
    MEMORY    = 1
    FILTERED  = 2
    PROJECTED = 3

def load_data(con, data_dir: str, source: Source):
    tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
    if source == Source.PARQUET: # Parquet
        for table in tables:
            con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{data_dir}/tpch/{table}.parquet');")
    else:
        for table in tables:
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM read_parquet('{data_dir}/tpch/{table}.parquet');")
    
    if source == Source.FILTERED:
        for filename in glob(f"sql/tpch/filtered_views/*.sql"):
            with open(filename, "r") as f:
                con.execute(f.read())

def execute_query(con, query):
    with open(f"sql/tpch/queries/{query}.sql", "r") as f:
        con.execute(f.read()).fetchall()

def run_benchmark(data_dir: str, source: Source):
    result = []
    query_dir = "sql/tpch/queries/"
    if source == Source.FILTERED:
        query_dir = "sql/tpch/filtered_queries/"

    con = duckdb.connect(":memory:")

    con.execute("SET threads TO 1")
    con.execute("SET enable_object_cache TO false")

    load_data(con, data_dir, source)

    con.execute("PRAGMA enable_profiling='json'")
    
    queries = sorted(glob(query_dir + "*.sql"))
    for filename in queries:
        profile_log = "profile.log"
        con.execute(f"PRAGMA profile_output='{profile_log}'")
        print(filename)
        query_results = []
        for _ in list(range(5)):
            with open(filename, "r") as f:
                con.execute(f.read()).fetchall()

            with open(profile_log, "r") as f:
                profile_data = json.load(f)
                query_results.append(profile_data["cpu_time"])
        result.append(sum(query_results) / len(query_results))

    con.close()

    return result

if __name__ == "__main__":
    data_dir = "/mnt/ramdisk"

    parquet_times  = run_benchmark(data_dir, Source.PARQUET)
    memory_times   = run_benchmark(data_dir, Source.MEMORY)
    filtered_times = run_benchmark(data_dir, Source.FILTERED)

    num_queries = len(parquet_times)

    print(f"{'Query':<5}    {'Parquet (s)':>11}    {'Table (s)':>16}    {'Filtered (s)':>16}")
    print("-" * 60)

    memory_speedups = [(1.0 - a / b) * -100.0 for a, b in zip(memory_times, parquet_times)]
    filtered_speedups = [(1.0 - a / b) * -100.0 for a, b in zip(filtered_times, memory_times)]
    for i in list(range(num_queries)):
        q = f"Q{str(i + 1)}"
        print(f"{q:<5}    {parquet_times[i]:>11.4f}    {memory_times[i]:>6.4f} ({memory_speedups[i]:>6.2f}%)    {(filtered_times[i]):>6.4f} ({filtered_speedups[i]:>6.2f}%)")
    
    print("-" * 60)
    print(f"Average {sum(memory_speedups)/num_queries:>30.2f}% {sum(filtered_speedups)/num_queries:>18.2f}%")
