#!/bin/bash

set -euo pipefail

DATA_DIR="${DATA_DIR:-/tmp/duckdb-bench-data}"

if command -v numactl >/dev/null 2>&1; then
    per_query="numactl -m 0 -N 0 ./build/queries"
    throughput="numactl -m 0 -N 0 ./build/throughput"
else
    per_query="./build/queries"
    throughput="./build/throughput"
fi

mkdir -p "$DATA_DIR"

prepare_data() {
    local sf="$1"
    local fmt="$2"
    local ordering="${3:-default}"

    rm -rf "${DATA_DIR:?}"/*
    python3 generate.py -s "$sf" -f "$fmt" -o "$DATA_DIR" -O "$ordering"
}

mkdir -p measurements/throughput measurements/queries

# Figure 1(a)
prepare_data 10 parquet

sources=( parquet memory filtered )
for s in "${sources[@]}"
do
    $throughput -s "$s" -t 1-64 -S 3 -d "$DATA_DIR"
done

mkdir -p measurements/throughput/tpch-10
mv measurements/throughput/*.json measurements/throughput/tpch-10

# Figure 1(b)
prepare_data 30 parquet

sources=( parquet memory filtered )
for s in "${sources[@]}"
do
    for t in $(seq 1 64)
    do
        $throughput -s "$s" -t "$t" -S 4 -d "$DATA_DIR"
    done
done

mkdir -p measurements/throughput/tpch-30
mv measurements/throughput/*.json measurements/throughput/tpch-30

# Figure 2
mkdir -p measurements/queries
sources=( parquet memory filtered )
for s in "${sources[@]}"
do
    $per_query -s "$s" -t 1 -d "$DATA_DIR"
done

mkdir -p measurements/queries/tpch-30
mv measurements/queries/*.json measurements/queries/tpch-30

# Figure 3(a): CSV
prepare_data 10 csv

$throughput -s csv -t 1-64 -S 3 -d "$DATA_DIR"

# Figure 3(a): JSON
prepare_data 10 json

$throughput -s json -t 1-64 -S 3 -d "$DATA_DIR"

mkdir -p measurements/throughput/other-10
mv measurements/throughput/*.json measurements/throughput/other-10

# Figure 3(b): Random
prepare_data 30 parquet random

$per_query -s parquet -t 1 -d "$DATA_DIR"

mkdir -p measurements/queries/tpch-30-random
mv measurements/queries/*.json measurements/queries/tpch-30-random

# Figure 3(b): Sorted
prepare_data 30 parquet sorted

$per_query -s parquet -t 1 -d "$DATA_DIR"

mkdir -p measurements/queries/tpch-30-sorted
mv measurements/queries/*.json measurements/queries/tpch-30-sorted
