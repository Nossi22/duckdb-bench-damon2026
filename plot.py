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


def load_throughput_measurements(pattern):
    rows = []
    for filepath in glob.glob(pattern, recursive=True):
        try:
            with open(filepath, "r") as f:
                this_data = json.load(f)
                if isinstance(this_data, dict):
                    rows.append(this_data)
        except (OSError, json.JSONDecodeError):
            continue
    return rows


def plot_throughput_only(df, output_path, metric="qph"):
    if metric == "qph":
        required = {"threads", "streams", "runtime_sec"}
    else:
        required = {"threads", "streams", "median_net_gbps"}

    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required throughput fields: {sorted(missing)}")

    data = df.copy()
    data["source"] = data["source"] if "source" in data.columns else "parquet"

    data["threads"] = pd.to_numeric(data["threads"], errors="coerce")
    data["streams"] = pd.to_numeric(data["streams"], errors="coerce")

    if metric == "qph":
        data["runtime_sec"] = pd.to_numeric(data["runtime_sec"], errors="coerce")
        data = data[data["runtime_sec"] > 0]
        data["value"] = data["streams"] * 22 * 3600 / data["runtime_sec"]
        y_label = "Queries / h"
        y_formatter = FuncFormatter(lambda x, _: f"{x/1000:.0f}k" if x > 0 else "0")
    else:
        data["median_net_gbps"] = pd.to_numeric(data["median_net_gbps"], errors="coerce")
        data = data[data["median_net_gbps"] >= 0]
        data["value"] = data["median_net_gbps"]
        y_label = "Network throughput (Gbit/s)"
        y_formatter = FuncFormatter(lambda x, _: f"{x:.0f}")

    data = data.dropna(subset=["threads", "streams", "value"])
    if data.empty:
        raise ValueError("No valid throughput rows available after filtering")

    _, ax = plt.subplots(figsize=(8, 3.25))
    ax.set_axisbelow(True)
    ax.grid(axis='y')
    ax.grid(axis='x')
    ax.yaxis.set_major_formatter(y_formatter)
    ax.tick_params(axis='both', length=0)

    markers = ["o", "s", "^", "D", "v", "P", "X"]
    for idx, ((source, streams), subset) in enumerate(data.groupby(["source", "streams"])):
        subset = subset.sort_values("threads")
        ax.plot(
            subset["threads"],
            subset["value"],
            marker=markers[idx % len(markers)],
            label=f"{source}, {int(streams)} stream(s)",
            markersize=3,
        )

    if metric == "net":
        ax.axhline(y=100.0, color=COLOR_DECODE, linestyle="--", linewidth=1.2)
        ax.text(data["threads"].max(), 102.0, "100 Gbit/s", ha="right")

    ax.set_xlabel("Number of threads", fontweight="bold")
    ax.set_ylabel(y_label, fontweight="bold")
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, 1.2), ncol=3, frameon=False, prop={'weight': 'bold'})

    plt.tight_layout()
    plt.savefig(output_path, bbox_inches="tight")

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

def main():
    parser = argparse.ArgumentParser(description="Plot benchmark measurements")
    parser.add_argument(
        "--throughput-only",
        action="store_true",
        help="Plot throughput from available throughput JSON files only (works with partial runs)",
    )
    parser.add_argument(
        "--throughput-input",
        type=str,
        default=os.path.join("measurements", "throughput", "**", "*.json"),
        help="Glob pattern for throughput JSON files",
    )
    parser.add_argument(
        "--throughput-output",
        type=str,
        default=os.path.join("plots", "throughput_partial.pdf"),
        help="Output file path for throughput-only plot",
    )
    parser.add_argument(
        "--throughput-metric",
        type=str,
        default="qph",
        choices=["qph", "net"],
        help="Metric for --throughput-only: qph (queries/h) or net (median_net_gbps)",
    )
    parser.add_argument(
        "--plot-throughput",
        action="store_true",
        help="Enable throughput figures in the default full plotting flow",
    )

    args = parser.parse_args()

    if args.throughput_only:
        throughput_rows = load_throughput_measurements(args.throughput_input)
        if not throughput_rows:
            raise ValueError(f"No throughput JSON objects found for pattern: {args.throughput_input}")
        throughput_df = pd.DataFrame(throughput_rows)
        plot_throughput_only(throughput_df, args.throughput_output, metric=args.throughput_metric)
        return

    # Queries plots
    queries_data = []

    for filepath in glob.glob(os.path.join("measurements", "queries", "tpch-30", "*.json")):
        with open(filepath, "r") as f:
            this_data = json.load(f) # each file contains a JSON array of objects
            queries_data.extend(this_data)

    queries_df = pd.DataFrame(queries_data)

    query_required = {"threads", "query", "source", "cpu_time_sec"}
    if query_required.issubset(set(queries_df.columns)):
        plot_cpu_time(queries_df)
    else:
        print("Skipping cpu_time_stacked.pdf (missing query measurements in measurements/queries/tpch-30)")

    if args.plot_throughput:
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

        throughput_required = {"source", "streams", "threads", "runtime_sec"}
        if throughput_required.issubset(set(throughput_df_10.columns)) and throughput_required.issubset(set(throughput_df_30.columns)):
            plot_appetizer(throughput_df_10, throughput_df_30)
        else:
            print("Skipping latency_by_threads.pdf (missing throughput measurements in tpch-10/tpch-30)")

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

        csv_json_required = {"source", "streams", "threads", "runtime_sec"}
        query_compare_required = {"source", "threads", "query", "operators", "runtime_sec"}
        if (
            csv_json_required.issubset(set(throughput_df_10.columns))
            and query_compare_required.issubset(set(unsorted_df_10.columns))
            and query_compare_required.issubset(set(sorted_df_10.columns))
        ):
            plot_csv_json(throughput_df_10, unsorted_df_10, sorted_df_10)
        else:
            print("Skipping csv_json.pdf (missing csv/json or sorted/random query measurements)")

if __name__ == "__main__":
    os.makedirs("plots", exist_ok=True)
    main()
