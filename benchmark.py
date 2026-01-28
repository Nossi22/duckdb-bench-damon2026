import duckdb
import json
import os

def load_data(con, data_dir, is_parquet):
    tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
    if is_parquet: # Parquet
        for table in tables:
            con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{data_dir}/tpch/{table}.parquet');")
    else:
        for table in tables:
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM read_parquet('{data_dir}/tpch/{table}.parquet');")

def execute_query(con, query):
    with open(f"sql/tpch/queries/{query}.sql", "r") as f:
        con.execute(f.read()).fetchall()

def run_benchmark(is_parquet, data_dir):
    bench_name = "parquet" if is_parquet else "memory"
    result = []

    con = duckdb.connect(":memory:")

    con.execute("SET threads TO 1")
    con.execute("SET enable_object_cache TO false")

    load_data(con, data_dir, is_parquet)

    con.execute("PRAGMA enable_profiling='json'")
    con.execute(f"PRAGMA profile_output='profile.log'")
    for i in list(range(1, 23)):
        q = f"q{str(i).zfill(2)}"
        execute_query(con, q)

        with open("profile.log", "r") as f:
            profile_data = json.load(f)
            result.append(profile_data["cpu_time"])

    con.close()

    return result

if __name__ == "__main__":
    data_dir = "data"

    parquet_times = run_benchmark(True, data_dir)
    memory_times = run_benchmark(False, data_dir)

    print(f"{'Query':<10} {'Parquet (s)':<15} {'Table (s)':<15} {'Saved (%)':<10}")
    print("-" * 50)
    for i in list(range(22)):
        q = f"Q{str(i + 1)}"
        speedup = memory_times[i] / parquet_times[i] * 100
        print(f"{q:<10} {parquet_times[i]:<15.4f} {memory_times[i]:<15.4f} {speedup:<10.2f}")
