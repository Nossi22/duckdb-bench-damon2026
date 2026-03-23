import argparse
import glob
import json
import os
from collections import defaultdict

import matplotlib.pyplot as plt
import numpy as np


def load_measurements(path, mode):
    grouped = defaultdict(list)
    for file_path in glob.glob(os.path.join(path, "*.json")):
        with open(file_path, "r") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            continue
        source = data.get("source")
        threads = data.get("threads")
        streams = data.get("streams")
        runtimes = data.get("runtimes")
        if source not in ("minio", "parquet"):
            continue
        if not isinstance(threads, int) or not isinstance(streams, int):
            continue
        if not isinstance(runtimes, list) or not runtimes:
            continue
        if mode == "best":
            value = float(min(runtimes))
        else:
            value = float(sum(runtimes) / len(runtimes))
        grouped[(source, threads, streams)].append(value)

    reduced = {}
    for key, values in grouped.items():
        reduced[key] = float(sum(values) / len(values))
    return reduced


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--measurements-dir", default="measurements_clean/throughput_tpch30")
    parser.add_argument("--threads", default="8,16,32")
    parser.add_argument("--streams", default="1,2,3,4")
    parser.add_argument("--mode", choices=["avg", "best"], default="avg")
    parser.add_argument("--output", default="plots/minio_vs_local_by_threads_tpch30_avg.pdf")
    args = parser.parse_args()

    threads = [int(x) for x in args.threads.split(",") if x.strip()]
    streams = [int(x) for x in args.streams.split(",") if x.strip()]

    values = load_measurements(args.measurements_dir, args.mode)
    missing = [
        (src, t, s)
        for src in ("minio", "parquet")
        for t in threads
        for s in streams
        if (src, t, s) not in values
    ]
    if missing:
        raise RuntimeError(f"Missing measurements: {missing}")

    fig, axes = plt.subplots(1, len(streams), figsize=(4.4 * len(streams), 3.6), squeeze=False)
    axes = axes[0]
    width = 0.36

    for i, s in enumerate(streams):
        ax = axes[i]
        x = np.arange(len(threads))
        local = [values[("parquet", t, s)] for t in threads]
        network = [values[("minio", t, s)] for t in threads]

        ax.bar(x - width / 2, local, width, color="#BF616A", label=f"Local {args.mode} runtime (red)")
        ax.bar(x + width / 2, network, width, color="#5E81AC", label=f"Network {args.mode} runtime (blue)")
        ax.set_xticks(x)
        ax.set_xticklabels([str(t) for t in threads])
        ax.set_xlabel("Threads")
        if i == 0:
            ax.set_ylabel(f"{args.mode.capitalize()} runtime (seconds)")
        ax.set_title(f"{s} streams")
        ax.grid(axis="y", alpha=0.25)
        ax.set_axisbelow(True)

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", bbox_to_anchor=(0.5, -0.02), ncol=2, frameon=False)
    fig.suptitle(f"TPCH-30: MinIO vs Local by Threads ({args.mode} of runtimes list)")
    fig.subplots_adjust(bottom=0.22, top=0.82, wspace=0.28)

    out_dir = os.path.dirname(args.output)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    plt.savefig(args.output, bbox_inches="tight")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
