import argparse
import glob
import json
import os
import re

import matplotlib.pyplot as plt
import numpy as np


def load_latest_run(measurements_dir, source, threads):
    latest_path = None
    latest_mtime = -1
    latest_rows = None

    for path in glob.glob(os.path.join(measurements_dir, "*.json")):
        with open(path, "r") as f:
            rows = json.load(f)
        if not isinstance(rows, list) or not rows:
            continue
        if rows[0].get("source") != source or rows[0].get("threads") != threads:
            continue
        mtime = os.path.getmtime(path)
        if mtime > latest_mtime:
            latest_mtime = mtime
            latest_path = path
            latest_rows = rows

    if latest_rows is None:
        raise RuntimeError(f"No query measurement found for source={source}, threads={threads} in {measurements_dir}")
    return latest_path, latest_rows


def query_key(query_path):
    m = re.search(r"q(\d+)\.sql$", query_path)
    if not m:
        return 10**9
    return int(m.group(1))


def query_label(query_path):
    m = re.search(r"q(\d+)\.sql$", query_path)
    if not m:
        return query_path
    return f"Q{int(m.group(1))}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--measurements-dir", default="measurements/queries")
    parser.add_argument("--threads", type=int, required=True)
    parser.add_argument("--local-source", default="parquet")
    parser.add_argument("--minio-source", default="minio")
    parser.add_argument("--output", default="plots/per_query_local_red_minio_blue.pdf")
    args = parser.parse_args()

    local_path, local_rows = load_latest_run(args.measurements_dir, args.local_source, args.threads)
    minio_path, minio_rows = load_latest_run(args.measurements_dir, args.minio_source, args.threads)

    local = {r["query"]: r["runtime_sec"] for r in local_rows}
    minio = {r["query"]: r["runtime_sec"] for r in minio_rows}
    common_queries = sorted(set(local).intersection(minio), key=query_key)
    if not common_queries:
        raise RuntimeError("No overlapping queries between local and minio runs")

    labels = [query_label(q) for q in common_queries]
    local_vals = np.array([local[q] for q in common_queries], dtype=float)
    minio_vals = np.array([minio[q] for q in common_queries], dtype=float)
    local_pct = (local_vals / minio_vals) * 100.0
    local_part = np.minimum(local_pct, 100.0)
    network_part = 100.0 - local_part

    x = np.arange(len(common_queries))
    width = 0.62
    plt.figure(figsize=(11, 4))
    plt.bar(x, local_part, width, color="#BF616A", label="Local runtime (red)")
    plt.bar(x, network_part, width, bottom=local_part, color="#5E81AC", label="Network runtime (blue)")
    plt.xticks(x, labels, rotation=90)
    for i, p in enumerate(local_pct):
        plt.text(x[i], max(100.0, p) + 1.0, f"{p:.0f}%", ha="center", va="bottom", fontsize=8, color="#BF616A")
    plt.ylim(0, max(114, float(np.nanmax(local_pct)) + 10))
    plt.ylabel("Runtime (% of network)")
    plt.xlabel("TPC-H query")
    plt.legend(loc="upper center", bbox_to_anchor=(0.5, -0.34), ncol=2, frameon=False)
    plt.subplots_adjust(bottom=0.54, top=0.90)

    output_dir = os.path.dirname(args.output)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    plt.savefig(args.output)
    print(f"Wrote {args.output}")
    print(f"Local file: {local_path}")
    print(f"MinIO file: {minio_path}")


if __name__ == "__main__":
    main()
