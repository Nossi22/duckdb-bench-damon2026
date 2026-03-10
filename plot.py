import json
import glob
import os
import argparse
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np
from matplotlib.ticker import FuncFormatter
from matplotlib.patches import Patch

for font_file in glob.glob("resources/*.ttf"):
    font_manager.fontManager.addfont(font_file)
plt.rcParams["font.family"] = "Inter 18pt"

# --- Plot ---
plt.rcParams.update({
    "grid.linestyle": "-",
    "grid.alpha": 1.0,
    "grid.color": "#E5E5EA",
    "font.size": 12,
    "axes.spines.right": False,
    "axes.spines.left": False,
    "axes.edgecolor": "#E5E5EA",
    "text.color": "#3A3A3C",
    "axes.labelcolor": "#3A3A3C",
    "xtick.color": "#3A3A3C",
    "ytick.color": "#3A3A3C",
})

COLOR_QUERY = "#5E81AC"
COLOR_DECODE = "#BF616A"
COLOR_FILTERED = "#A3BE8C"

COLOR_CSV = "#D08770"
COLOR_JSON = "#B48EAD"

def plot_cpu_time(df):
    data = df.copy()
    data = data[data["threads"] == 1]

    # Extract short query names: q01.sql -> Q1, q02.sql -> Q2, etc.
    data["query_label"] = data["query"].str.extract(r'q(\d+)\.sql')[0].astype(int).apply(lambda x: f"Q{x}")

    # Pivot to get parquet and memory side by side
    pivot = data.pivot_table(index="query_label", columns="source", values="cpu_time_sec")
    pivot = pivot.loc[sorted(pivot.index, key=lambda s: int(s[1:]))]

    # The "memory" portion is the base (compute time on in-memory data)
    # The "parquet" overhead is the difference: parquet - memory (decoding cost)
    pivot["filter_pct"] = ((pivot["memory"] - pivot["filtered"]) / pivot["parquet"]) * 100
    pivot["decode_pct"] = ((pivot["parquet"] - pivot["memory"]) / pivot["parquet"]) * 100
    pivot["query_pct"] = (pivot["filtered"] / pivot["parquet"]) * 100

    _, ax = plt.subplots(figsize=(8, 3.25))

    ax.set_axisbelow(True)
    ax.grid(axis='y')
    ax.grid(axis='x', visible=False)
    ax.set_ylim(0, 100)
    ax.set_xlim(-0.8, len(pivot.index) + 1.0)
    ax.tick_params(axis='both', length=0)

    x = np.arange(len(pivot.index))
    width = 0.5

    ax.bar(x, pivot["decode_pct"], width, label="Parquet decoding", color=COLOR_DECODE)
    ax.bar(x, pivot["filter_pct"], width, bottom=pivot["decode_pct"], label="Filtering", color=COLOR_FILTERED)
    ax.bar(x, [100 for _ in range(len(pivot.index))], width, bottom=pivot["decode_pct"] + pivot["filter_pct"], label="Remaining query", color=COLOR_QUERY)

    avg_query = 100.0 - pivot["query_pct"].mean()
    avg_decode = pivot["decode_pct"].mean()
    ax.axhline(y=avg_query, color=COLOR_FILTERED, linestyle="--", linewidth=1.5)
    ax.axhline(y=avg_decode, color=COLOR_DECODE, linestyle="--", linewidth=1.5)

    ax.text(len(pivot.index) - 0.5, avg_query + 1.5, f"{avg_query:.0f}%", ha="left")
    ax.text(len(pivot.index) - 0.5, avg_decode + 1.5, f"{avg_decode:.0f}%", ha="left")

    ax.set_xticks(x)
    ax.set_xticklabels(pivot.index, rotation=90)
    ax.set_xlabel("TPC-H query", fontweight="bold")
    ax.set_ylabel("CPU time (%)", fontweight="bold")
    ax.legend(loc="lower center", bbox_to_anchor=(0.5, 1.0), ncol=3, frameon=False, prop={'weight': 'bold'}, columnspacing=1.2, handletextpad=0.3, handlelength=1.0)

    plt.tight_layout()
    plt.savefig("plots/cpu_time_stacked.pdf", bbox_inches="tight")

def plot_appetizer(df_10, df_30):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8, 3.25))
    
    for ax, data, sf, label, streams in [(ax1, df_10.copy(), 10, "a", 3), (ax2, df_30.copy(), 30, "b", 4)]:
        ax.set_axisbelow(True)
        ax.grid(axis='y')
        ax.grid(axis='x')
        ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x/1000:.0f}k" if x > 0 else "0"))
        ax.set_xlim(0, 65)
        ax.tick_params(axis='both', length=0)
        
        factor = streams * 22 * 3600
        subset = data[(data["source"] == "parquet") & (data["streams"] == streams)].sort_values("threads")
        ax.plot(subset["threads"], factor / subset["runtime_sec"], marker="s", color=COLOR_DECODE, 
                label="Parquet files", markersize=2)
        
        subset = data[(data["source"] == "memory") & (data["streams"] == streams)].sort_values("threads")
        ax.plot(subset["threads"], factor / subset["runtime_sec"], marker="o", color=COLOR_FILTERED, 
                label="Tables", markersize=2)
        
        subset = data[(data["source"] == "filtered") & (data["streams"] == streams)].sort_values("threads")
        ax.plot(subset["threads"], factor / subset["runtime_sec"], marker="^", color=COLOR_QUERY, 
                label="Pre-filtered tables", markersize=2)
        
        memory_val = factor / data[(data["source"] == "filtered") & (data["threads"] == 16) & (data["streams"] == streams)]["runtime_sec"].values[0]
        ax.axhline(y=memory_val, color=COLOR_QUERY, linestyle="--")
        ax.text(subset["threads"].values[-1] * 0.8, memory_val + (ax.get_ylim()[1] - ax.get_ylim()[0]) * 0.02, f"{memory_val:.0f} Q/h", ha="left",
                fontsize=10)

        if sf == 10:
            ax.annotate("16 threads",
                xy=(16, memory_val),
                xytext=(10, memory_val + (ax.get_ylim()[1] - ax.get_ylim()[0]) * 0.15),
                arrowprops=dict(arrowstyle="->", color="#3A3A3C", lw=1.0),
                fontsize=10, ha="center",
            )
        
        ax.set_xlabel("Number of threads", fontweight="bold")
        ticks = sorted(data["threads"].unique())
        ticks = [t for t in ticks if t == 1 or t % 8 == 0]
        ax.set_xticks(ticks)
        ax.set_xticklabels(ticks)
        ax.set_title(f"({label}) TPC-H scale factor {sf}", fontsize=12, y=-0.35)
    
    ax1.set_ylabel("Queries / h", fontweight="bold")

    # Single shared legend on top
    handles, labels = ax1.get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 1.07), ncol=3, frameon=False, prop={'weight': 'bold'}, columnspacing=1.2, handletextpad=0.3, handlelength=1.0)
    
    plt.tight_layout()
    plt.savefig("plots/latency_by_threads.pdf", bbox_inches="tight")

def plot_csv_json(df_10, df_10_unsorted, df_10_sorted):
    _, (ax1, ax2) = plt.subplots(1, 2, figsize=(8, 3.75), gridspec_kw={'width_ratios': [0.4, 0.6]})

    for i, ax, df, title in [(0, ax1, df_10, "(a) CSV & JSON parsing"), (1, ax2, (df_10_unsorted, df_10_sorted), "(b) Parquet input ordering & row group pruning")]:
        ax.set_axisbelow(True)
        ax.grid(axis='y')
        ax.grid(axis='x')

        if i == 0:
            data = df.copy()

            ax.set_ylim(0, 4000)

            ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x/1000:.0f}k" if x > 0 else "0"))
            ax.set_xlim(0, 65)
            ax.tick_params(axis='both', length=0)
            
            factor = 3 * 22 * 3600
            subset = data[(data["source"] == "csv") & (data["streams"] == 3)].sort_values("threads")
            ax.plot(subset["threads"], factor / subset["runtime_sec"], marker="s", color=COLOR_CSV, 
                    label="CSV files", markersize=2)
            
            subset = data[(data["source"] == "json") & (data["streams"] == 3)].sort_values("threads")
            ax.plot(subset["threads"], factor / subset["runtime_sec"], marker="o", color=COLOR_JSON, 
                    label="JSON files", markersize=2)
            
            ax.set_xlabel("Number of threads", fontweight="bold")
            ticks = sorted(data["threads"].unique())
            ticks = [t for t in ticks if t == 1 or t % 8 == 0]
            ax.set_xticks(ticks)
            ax.set_xticklabels(ticks)
            ax.set_ylabel("Queries / h", fontweight="bold")

            ax.legend(loc="upper center", bbox_to_anchor=(0.5, 1.2), ncol=3, frameon=False, prop={'weight': 'bold'}, columnspacing=1.2, handletextpad=0.3, handlelength=1.0)
        else:
            data_unsorted = df[0].copy()
            data_sorted = df[1].copy()
            ax.tick_params(axis='both', length=0)
            ax.set_ylim(0, 100)
            ax.grid(axis='x', visible=False)
            
            data_unsorted = data_unsorted[(data_unsorted["source"] == "parquet") & (data_unsorted["threads"] == 1)]
            data_sorted = data_sorted[(data_sorted["source"] == "parquet") & (data_sorted["threads"] == 1)]

            data_unsorted["query_label"] = data_unsorted["query"].str.extract(r'q(\d+)\.sql')[0].astype(int).apply(lambda x: f"Q{x}")
            data_sorted["query_label"] = data_sorted["query"].str.extract(r'q(\d+)\.sql')[0].astype(int).apply(lambda x: f"Q{x}")
    
            keep = ["Q1", "Q2", "Q3", "Q6", "Q7", "Q10", "Q12", "Q14", "Q15", "Q16", "Q20"]

            data_unsorted = data_unsorted[data_unsorted["query_label"].isin(keep)].sort_values("query")
            data_sorted = data_sorted[data_sorted["query_label"].isin(keep)].sort_values("query")

            queries = data_unsorted.sort_values("query")["query_label"].values

            ax.set_xlim(-.5, len(queries) - .5)

            bar_width = 0.25

            for idx, (label, data) in enumerate([("U", data_unsorted), ("Sorted", data_sorted)]):
                data = data.sort_values("query")
                scan = data["operators"].apply(lambda o: o["scan"]).values
                filt = data["operators"].apply(lambda o: o["filter"]).values
                scan_filter = scan + filt
                rest = data["runtime_sec"].values - scan_filter

                if idx == 0:
                    # Compute max runtime per query across both datasets
                    sorted_tmp = data_sorted.sort_values("query")
                    max_runtime = np.maximum(data["runtime_sec"].values, sorted_tmp["runtime_sec"].values)
                # Normalize
                scan_filter = scan_filter / max_runtime * 100
                rest = rest / max_runtime * 100

                x = np.arange(len(queries))
                
                offset = idx * (bar_width + 0.05) - (bar_width + 0.05) / 2

                hatch = '////' if label == "Sorted" else None
                ax.bar(x + offset, scan_filter, bar_width, label=f"Scan", color=COLOR_DECODE, hatch=hatch, edgecolor='#3A3A3C', linewidth=0)
                ax.bar(x + offset, rest, bar_width, bottom=scan_filter, label=f"Rest", color=COLOR_QUERY, hatch=hatch, edgecolor='#3A3A3C', linewidth=0)

            ax.set_xlabel("TPC-H query", fontweight="bold")
            ax.set_xticks(np.arange(len(queries)))
            ax.set_xticklabels(queries)
            ax.set_ylabel("Relative runtime (%)", fontweight="bold")

            legend_handles = [
                Patch(facecolor=COLOR_DECODE, label='Scan'),
                Patch(facecolor=COLOR_QUERY, label='Rest'),
                Patch(facecolor='white', edgecolor='#3A3A3C', linewidth=1, label='Unsorted'),
                Patch(facecolor='white', edgecolor='#3A3A3C', linewidth=1, hatch='/////', label='Sorted'),
            ]
            ax.legend(handles=legend_handles, loc="upper center", bbox_to_anchor=(0.5, 1.2), ncol=4, frameon=False, prop={'weight': 'bold'}, columnspacing=1.2, handletextpad=0.3, handlelength=1.0)
        ax.set_title(title, fontsize=12, y=-0.4)

    plt.tight_layout()
    plt.savefig("plots/csv_json.pdf", bbox_inches="tight")

def plot_minio_local_runtime_comparison(measurements_dir, local_source, threads, streams, output_file):
    records = []
    for filepath in glob.glob(os.path.join(measurements_dir, "**", "*.json"), recursive=True):
        with open(filepath, "r") as f:
            obj = json.load(f)
        if isinstance(obj, dict) and "source" in obj and "runtime_sec" in obj:
            obj["_path"] = filepath
            obj["_mtime"] = os.path.getmtime(filepath)
            records.append(obj)

    if not records:
        raise RuntimeError(f"No throughput JSON files found under {measurements_dir}")

    df = pd.DataFrame(records)
    df = df[df["source"].isin(["minio", local_source])]
    df = df[df["threads"].isin(threads) & df["streams"].isin(streams)]
    if df.empty:
        raise RuntimeError("No matching records for selected sources/threads/streams")

    latest = (
        df.sort_values("_mtime")
        .groupby(["source", "threads", "streams"], as_index=False)
        .tail(1)
    )

    minio = latest[latest["source"] == "minio"][["threads", "streams", "runtime_sec"]].rename(
        columns={"runtime_sec": "runtime_minio"}
    )
    local = latest[latest["source"] == local_source][["threads", "streams", "runtime_sec"]].rename(
        columns={"runtime_sec": "runtime_local"}
    )
    merged = minio.merge(local, on=["threads", "streams"], how="outer")
    merged = merged[merged["threads"].isin(threads) & merged["streams"].isin(streams)]
    if merged.empty:
        raise RuntimeError("No minio/parquet pairs found for selected filters")
    merged["local_pct_vs_minio"] = merged["runtime_local"] / merged["runtime_minio"] * 100.0

    fig, axes = plt.subplots(1, len(threads), figsize=(4.2 * len(threads), 3.6), squeeze=False)
    axes = axes[0]

    for idx, t in enumerate(threads):
        ax = axes[idx]
        ax.set_axisbelow(True)
        ax.grid(axis="y")
        ax.grid(axis="x", visible=False)
        ax.tick_params(axis="both", length=0)

        subset = merged[merged["threads"] == t].sort_values("streams")
        x = np.arange(len(streams))
        minio_runtime = []
        local_runtime = []
        for s in streams:
            row = subset[subset["streams"] == s]
            minio_runtime.append(float(row["runtime_minio"].iloc[0]) if not row.empty and pd.notna(row["runtime_minio"].iloc[0]) else np.nan)
            local_runtime.append(float(row["runtime_local"].iloc[0]) if not row.empty and pd.notna(row["runtime_local"].iloc[0]) else np.nan)

        low_runtime =  minio_runtime
        delta_runtime = np.abs(np.array(minio_runtime) - np.array(local_runtime))
        red = ax.bar(x, low_runtime, width=0.58, color="#BF616A", label="Network Runtime (s)")
        blue = ax.bar(x, delta_runtime, bottom=low_runtime, width=0.58, color="#5E81AC", label="Local Runtime (s)")
        for i, (b, r) in enumerate(zip(local_runtime, minio_runtime)):
            if np.isnan(b) and np.isnan(r):
                ax.text(x[i], 0.02, "n/a", ha="center", va="bottom", fontsize=8, transform=ax.get_xaxis_transform())
            elif not np.isnan(b) and not np.isnan(r) and r != 0:
                pct = (b / r) * 100.0
                top_of_delta = low_runtime[i] + delta_runtime[i]
                ax.text(x[i], top_of_delta + 0.18, f"{pct:.0f}%", ha="center", va="bottom", fontsize=8, color="#BF616A")

        ax.set_xticks(x)
        ax.set_xticklabels([str(s) for s in streams])
        ax.set_xlabel("Concurrent streams", fontweight="bold")
        ax.set_ylabel("Runtime (seconds)", fontweight="bold")
        ax.set_title(f"{t} threads")

        if idx == len(threads) - 1:
            fig.legend([red, blue], ["Network Runtime part (red)", "Local Runtime part (blue)"], loc="lower center",
                       bbox_to_anchor=(0.5, -0.02), ncol=2, frameon=False)

    fig.suptitle(f"Runtime comparison bars: {local_source} vs MinIO", y=0.98)
    fig.subplots_adjust(bottom=0.22, top=0.82, wspace=0.30)
    plt.savefig(output_file, bbox_inches="tight")

def main():
    # Queries plots
    queries_data = []

    for filepath in glob.glob(os.path.join("measurements", "queries", "tpch-30", "*.json")):
        with open(filepath, "r") as f:
            this_data = json.load(f) # each file contains a JSON array of objects
            queries_data.extend(this_data)

    queries_df = pd.DataFrame(queries_data)

    plot_cpu_time(queries_df)

    # Throughput plots
    throughput_data_10 = []
    throughput_data_30 = []

    for filepath in glob.glob(os.path.join("measurements", "throughput", "tpch-10", "*.json")):
        with open(filepath, "r") as f:
            this_data = json.load(f) # each file contains a JSON object
            throughput_data_10.append(this_data)
    for filepath in glob.glob(os.path.join("measurements", "throughput", "tpch-30", "*.json")):
        with open(filepath, "r") as f:
            this_data = json.load(f) # each file contains a JSON object
            throughput_data_30.append(this_data)
    
    throughput_df_10 = pd.DataFrame(throughput_data_10)
    throughput_df_30 = pd.DataFrame(throughput_data_30)

    plot_appetizer(throughput_df_10, throughput_df_30)

    # CSV and JSON plot
    throughput_data_10 = []
    unsorted_data_10 = []
    sorted_data_10 = []

    for filepath in glob.glob(os.path.join("measurements", "throughput", "other-10", "*.json")):
        with open(filepath, "r") as f:
            this_data = json.load(f) # each file contains a JSON object
            throughput_data_10.append(this_data)
    for filepath in glob.glob(os.path.join("measurements", "queries", "tpch-30-random", "*.json")):
        with open(filepath, "r") as f:
            this_data = json.load(f) # each file contains a JSON object
            unsorted_data_10.extend(this_data)
    for filepath in glob.glob(os.path.join("measurements", "queries", "tpch-30-sorted", "*.json")):
        with open(filepath, "r") as f:
            this_data = json.load(f) # each file contains a JSON object
            sorted_data_10.extend(this_data)
    
    throughput_df_10 = pd.DataFrame(throughput_data_10)
    unsorted_df_10 = pd.DataFrame(unsorted_data_10)
    sorted_df_10 = pd.DataFrame(sorted_data_10)

    plot_csv_json(throughput_df_10, unsorted_df_10, sorted_df_10)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--compare-minio-local", action="store_true")
    parser.add_argument("--measurements-dir", default="measurements/throughput")
    parser.add_argument("--local-source", default="parquet")
    parser.add_argument("--threads", default="8,16,32")
    parser.add_argument("--streams", default="1,2,3,4")
    parser.add_argument("--output", default="plots/minio_local_runtime_pct.pdf")
    args = parser.parse_args()

    os.makedirs("plots", exist_ok=True)
    if args.compare_minio_local:
        threads = [int(x.strip()) for x in args.threads.split(",") if x.strip()]
        streams = [int(x.strip()) for x in args.streams.split(",") if x.strip()]
        plot_minio_local_runtime_comparison(
            measurements_dir=args.measurements_dir,
            local_source=args.local_source,
            threads=threads,
            streams=streams,
            output_file=args.output,
        )
    else:
        main()
