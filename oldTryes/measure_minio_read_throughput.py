import argparse
import csv
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import urlparse

import duckdb

try:
    import boto3
except ImportError:
    boto3 = None


@dataclass
class EndpointConfig:
    host: str
    port: int
    use_ssl: bool

    @property
    def duckdb_s3_endpoint(self) -> str:
        return f"{self.host}:{self.port}"

    @property
    def boto_endpoint_url(self) -> str:
        scheme = "https" if self.use_ssl else "http"
        return f"{scheme}://{self.host}:{self.port}"


def parse_endpoint(ip_or_url: str, default_port: int) -> EndpointConfig:
    raw = ip_or_url.strip()
    if not raw:
        raise ValueError("--minio-ip must not be empty")

    if "://" in raw:
        parsed = urlparse(raw)
        if parsed.hostname is None:
            raise ValueError(f"Could not parse host from endpoint: {ip_or_url}")
        host = parsed.hostname
        port = parsed.port if parsed.port is not None else default_port
        use_ssl = parsed.scheme == "https"
        return EndpointConfig(host=host, port=port, use_ssl=use_ssl)

    return EndpointConfig(host=raw, port=default_port, use_ssl=False)


def list_parquet_objects(
    endpoint: EndpointConfig,
    bucket: str,
    access_key: str,
    secret_key: str,
    prefix: str,
) -> List[Dict[str, object]]:
    if boto3 is None:
        raise RuntimeError("boto3 is required for MinIO benchmarking. Install requirements.txt first.")

    session = boto3.session.Session()
    client = session.client(
        "s3",
        endpoint_url=endpoint.boto_endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    normalized_prefix = prefix.strip("/")
    paginator = client.get_paginator("list_objects_v2")

    objects = []
    pagination_args = {"Bucket": bucket}
    if normalized_prefix:
        pagination_args["Prefix"] = normalized_prefix + "/"

    for page in paginator.paginate(**pagination_args):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                objects.append({"key": key, "size": int(obj["Size"])})

    if not objects:
        raise ValueError(
            f"No parquet objects found in s3://{bucket}/{normalized_prefix if normalized_prefix else ''}"
        )

    return sorted(objects, key=lambda item: item["key"])


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def configure_connection(
    conn: duckdb.DuckDBPyConnection,
    endpoint: EndpointConfig,
    access_key: str,
    secret_key: str,
):
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")
    conn.execute("SET enable_object_cache=false")
    conn.execute("SET s3_url_style='path'")
    conn.execute("SET s3_region='us-east-1'")
    conn.execute(f"SET s3_endpoint='{endpoint.duckdb_s3_endpoint}'")
    conn.execute(f"SET s3_use_ssl={'true' if endpoint.use_ssl else 'false'}")
    conn.execute(f"SET s3_access_key_id='{access_key}'")
    conn.execute(f"SET s3_secret_access_key='{secret_key}'")


def create_views(
    conn: duckdb.DuckDBPyConnection,
    bucket: str,
    objects: List[Dict[str, object]],
) -> List[Tuple[str, str, int]]:
    tables = []
    for obj in objects:
        key = obj["key"]
        size = int(obj["size"])
        table_name = Path(key).stem
        s3_path = f"s3://{bucket}/{key}"
        conn.execute(
            f"CREATE OR REPLACE VIEW {quote_ident(table_name)} AS "
            f"SELECT * FROM read_parquet('{s3_path}')"
        )
        tables.append((table_name, s3_path, size))
    return tables


def build_full_scan_queries(
    conn: duckdb.DuckDBPyConnection,
    table_names: List[str],
) -> Dict[str, str]:
    queries = {}
    for table_name in table_names:
        info = conn.execute(f"PRAGMA table_info({quote_ident(table_name)})").fetchall()
        columns = [quote_ident(row[1]) for row in info]
        if not columns:
            raise ValueError(f"No columns found for view {table_name}")
        col_expr = ", ".join(columns)
        queries[table_name] = f"SELECT SUM(hash({col_expr})) FROM {quote_ident(table_name)}"
    return queries


def run_worker(
    endpoint: EndpointConfig,
    bucket: str,
    access_key: str,
    secret_key: str,
    objects: List[Dict[str, object]],
    rounds: int,
    warmup_rounds: int,
    db_threads: int,
) -> Tuple[float, int]:
    conn = duckdb.connect(":memory:")
    conn.execute(f"SET threads={db_threads}")
    configure_connection(conn, endpoint, access_key, secret_key)
    tables = create_views(conn, bucket, objects)
    table_names = [t[0] for t in tables]
    per_round_bytes = sum(t[2] for t in tables)
    queries = build_full_scan_queries(conn, table_names)

    for _ in range(warmup_rounds):
        for table_name in table_names:
            conn.execute(queries[table_name]).fetchone()

    start = time.perf_counter()
    for _ in range(rounds):
        for table_name in table_names:
            conn.execute(queries[table_name]).fetchone()
    elapsed = time.perf_counter() - start
    conn.close()

    return elapsed, per_round_bytes * rounds


def run_worker_group(
    workers: int,
    endpoint: EndpointConfig,
    bucket: str,
    access_key: str,
    secret_key: str,
    objects: List[Dict[str, object]],
    rounds: int,
    warmup_rounds: int,
    db_threads: int,
):
    wall_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                run_worker,
                endpoint,
                bucket,
                access_key,
                secret_key,
                objects,
                rounds,
                warmup_rounds,
                db_threads,
            )
            for _ in range(workers)
        ]
        results = [f.result() for f in futures]

    wall_elapsed = time.perf_counter() - wall_start
    total_bytes = sum(item[1] for item in results)
    throughput_gbps = (total_bytes * 8.0) / wall_elapsed / 1e9
    throughput_gBps = total_bytes / wall_elapsed / 1e9

    return {
        "workers": workers,
        "rounds": rounds,
        "warmup_rounds": warmup_rounds,
        "db_threads_per_worker": db_threads,
        "elapsed_sec": wall_elapsed,
        "total_bytes": total_bytes,
        "throughput_gbps": throughput_gbps,
        "throughput_gBps": throughput_gBps,
        "target_100gbps_reached": throughput_gbps >= 100.0,
    }


def parse_workers(value: str) -> List[int]:
    parts = [item.strip() for item in value.split(",") if item.strip()]
    workers = sorted({int(item) for item in parts})
    if not workers or any(w <= 0 for w in workers):
        raise ValueError("--workers must contain positive integers, e.g. 1,2,4,8")
    return workers


def ensure_output_dir(path: str):
    Path(path).mkdir(parents=True, exist_ok=True)


def main():
    parser = argparse.ArgumentParser(
        description="Measure MinIO read throughput via DuckDB views over s3:// parquet files"
    )
    parser.add_argument("--minio-ip", type=str, required=True, help="MinIO IP or URL")
    parser.add_argument("--minio-port", type=int, default=9000, help="MinIO port (default: 9000)")
    parser.add_argument("--minio-access-key", type=str, default="minioadmin", help="MinIO access key")
    parser.add_argument("--minio-secret-key", type=str, default="minioadmin", help="MinIO secret key")
    parser.add_argument("--bucket", type=str, default="tpch", help="MinIO bucket (default: tpch)")
    parser.add_argument("--prefix", type=str, default="", help="Optional object prefix inside bucket")
    parser.add_argument(
        "--workers",
        type=str,
        default="1,2,4,8",
        help="Comma-separated worker counts, e.g. 1,2,4,8,16",
    )
    parser.add_argument("--rounds", type=int, default=2, help="Measurement rounds per worker (default: 2)")
    parser.add_argument("--warmup-rounds", type=int, default=1, help="Warmup rounds per worker (default: 1)")
    parser.add_argument(
        "--db-threads-per-worker",
        type=int,
        default=1,
        help="DuckDB threads per worker connection (default: 1)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="measurements/network",
        help="Output directory for CSV+JSON results",
    )

    args = parser.parse_args()

    if args.rounds <= 0 or args.warmup_rounds < 0:
        raise ValueError("--rounds must be > 0 and --warmup-rounds must be >= 0")
    if args.db_threads_per_worker <= 0:
        raise ValueError("--db-threads-per-worker must be > 0")

    workers = parse_workers(args.workers)
    endpoint = parse_endpoint(args.minio_ip, args.minio_port)

    objects = list_parquet_objects(
        endpoint=endpoint,
        bucket=args.bucket,
        access_key=args.minio_access_key,
        secret_key=args.minio_secret_key,
        prefix=args.prefix,
    )

    total_dataset_bytes = sum(int(obj["size"]) for obj in objects)
    print(f"Found {len(objects)} parquet object(s), dataset size: {total_dataset_bytes / 1e9:.3f} GB")

    rows = []
    for worker_count in workers:
        print(f"Running read-throughput benchmark for workers={worker_count} ...")
        row = run_worker_group(
            workers=worker_count,
            endpoint=endpoint,
            bucket=args.bucket,
            access_key=args.minio_access_key,
            secret_key=args.minio_secret_key,
            objects=objects,
            rounds=args.rounds,
            warmup_rounds=args.warmup_rounds,
            db_threads=args.db_threads_per_worker,
        )
        row["timestamp_utc"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        row["bucket"] = args.bucket
        row["prefix"] = args.prefix
        row["minio_endpoint"] = endpoint.boto_endpoint_url
        row["dataset_bytes_per_round"] = total_dataset_bytes
        print(
            f"  -> {row['throughput_gbps']:.2f} Gbit/s ({row['throughput_gBps']:.2f} GB/s), "
            f"elapsed={row['elapsed_sec']:.2f}s"
        )
        rows.append(row)

    ensure_output_dir(args.output_dir)
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    csv_path = Path(args.output_dir) / f"minio_read_throughput_{stamp}.csv"
    json_path = Path(args.output_dir) / f"minio_read_throughput_{stamp}.json"

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    with open(json_path, "w") as f:
        json.dump(rows, f, indent=2)

    best = max(rows, key=lambda item: item["throughput_gbps"])
    print(f"\nBest throughput: {best['throughput_gbps']:.2f} Gbit/s with workers={best['workers']}")
    print(f"100 Gbit/s reached: {'YES' if best['target_100gbps_reached'] else 'NO'}")
    print(f"Wrote: {csv_path}")
    print(f"Wrote: {json_path}")


if __name__ == "__main__":
    main()
