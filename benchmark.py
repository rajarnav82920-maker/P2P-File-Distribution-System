"""
P2P Performance Benchmarking & Evaluation Suite.

Evaluates:
- Download Time vs Number of Peers
- Throughput (KB/s, MB/s) vs Number of Peers
- Performance scaling with different File Sizes (e.g. 512 KB, 2 MB, 8 MB)
- Generates data tables via pandas and visualization charts via matplotlib
"""

import os
import sys
import time
import shutil
import argparse
from typing import List, Dict, Any

# Ensure matplotlib writes cache to a writable local temp folder
os.environ["MPLCONFIGDIR"] = os.path.abspath(os.path.join(os.path.dirname(__file__), ".mpl_cache"))

import pandas as pd
import matplotlib
matplotlib.use("Agg")  # Non-interactive backend suitable for headless rendering
import matplotlib.pyplot as plt

from src.peer import PeerNode
from src.tracker import TrackerServer
from src.config import DEFAULT_CHUNK_SIZE


def generate_benchmark_file(path: str, size_bytes: int) -> None:
    """Generates a pseudo-random patterned binary file of exact size."""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    block = b"Antigravity-P2P-Chunk-Benchmark-Block-0123456789ABCDEF\n" * 64  # ~3.5 KB
    written = 0
    with open(path, "wb") as f:
        while written < size_bytes:
            to_write = min(len(block), size_bytes - written)
            f.write(block[:to_write])
            written += to_write


def run_single_test(
    file_size_bytes: int,
    num_seeders: int,
    tracker_port: int = 9000,
    base_seeder_port: int = 9100,
    downloader_port: int = 9200,
    test_run_dir: str = "benchmark_scratch",
) -> Dict[str, Any]:
    """
    Executes a single benchmark run:
    1. Sets up Tracker
    2. Sets up `num_seeders` peers with identical file
    3. Runs downloader peer to swarm-download the file
    4. Records metrics
    5. Cleans up
    """
    os.makedirs(test_run_dir, exist_ok=True)
    file_name = f"bench_{file_size_bytes // 1024}KB.dat"
    master_file_path = os.path.join(test_run_dir, "master", file_name)
    generate_benchmark_file(master_file_path, file_size_bytes)

    # 1. Start Tracker
    tracker = TrackerServer(host="127.0.0.1", port=tracker_port)
    tracker.start()
    time.sleep(0.3)

    seeders: List[PeerNode] = []
    downloader: PeerNode = None

    try:
        # 2. Start Seeders
        for i in range(num_seeders):
            s_port = base_seeder_port + i
            s_id = f"Seeder_{s_port}"
            s_shared = os.path.join(test_run_dir, f"seeder_{s_port}_shared")
            s_dl = os.path.join(test_run_dir, f"seeder_{s_port}_dl")
            os.makedirs(s_shared, exist_ok=True)

            # Copy master file into seeder shared dir
            shutil.copy(master_file_path, os.path.join(s_shared, file_name))

            seeder = PeerNode(
                peer_id=s_id,
                host="127.0.0.1",
                port=s_port,
                tracker_host="127.0.0.1",
                tracker_port=tracker_port,
                shared_dir=s_shared,
                downloads_dir=s_dl,
            )
            seeder.start()
            seeders.append(seeder)

        time.sleep(0.5)

        # 3. Start Downloader Peer
        dl_shared = os.path.join(test_run_dir, "downloader_shared")
        dl_target = os.path.join(test_run_dir, "downloader_dl")
        shutil.rmtree(dl_target, ignore_errors=True)
        os.makedirs(dl_target, exist_ok=True)

        downloader = PeerNode(
            peer_id="Benchmark_Downloader",
            host="127.0.0.1",
            port=downloader_port,
            tracker_host="127.0.0.1",
            tracker_port=tracker_port,
            shared_dir=dl_shared,
            downloads_dir=dl_target,
        )
        downloader.start()
        time.sleep(0.3)

        # 4. Perform Download
        t0 = time.perf_counter()
        success, final_path, metrics = downloader.download_file(file_name)
        total_wall_time = time.perf_counter() - t0

        if not success or not metrics:
            raise RuntimeError(f"Download failed for {file_name} with {num_seeders} seeders")

        result = {
            "file_name": file_name,
            "file_size_kb": file_size_bytes / 1024.0,
            "file_size_mb": file_size_bytes / (1024.0 * 1024.0),
            "num_seeders": num_seeders,
            "total_chunks": metrics.total_chunks,
            "successful_chunks": metrics.successful_chunks,
            "retries": metrics.retry_count,
            "elapsed_time_sec": round(metrics.elapsed_time, 4),
            "throughput_mbps": round(metrics.throughput_mbps, 3),
            "throughput_kbps": round(metrics.throughput_kbps, 2),
        }
        return result

    finally:
        # Graceful cleanup
        if downloader:
            downloader.stop()
        for s in seeders:
            s.stop()
        tracker.stop()
        time.sleep(0.5)
        shutil.rmtree(test_run_dir, ignore_errors=True)


def run_benchmark_suite(quick: bool = False) -> pd.DataFrame:
    """
    Runs a grid of experiments across file sizes and peer counts.
    """
    if quick:
        file_sizes = [512 * 1024, 2 * 1024 * 1024]      # 512 KB, 2 MB
        peer_counts = [1, 2, 4]                          # 1, 2, 4 seeders
    else:
        file_sizes = [512 * 1024, 2 * 1024 * 1024, 8 * 1024 * 1024]  # 512 KB, 2 MB, 8 MB
        peer_counts = [1, 2, 3, 4]

    results = []
    total_runs = len(file_sizes) * len(peer_counts)
    current_run = 0

    print("=" * 68)
    print("      P2P SWARM PERFORMANCE BENCHMARKING SUITE")
    print("=" * 68)
    print(f" Total Test Cases : {total_runs}")
    print(f" File Sizes       : {[f'{s // 1024} KB' for s in file_sizes]}")
    print(f" Peer Counts      : {peer_counts}")
    print("=" * 68)

    base_port_offset = 0

    for f_size in file_sizes:
        for p_count in peer_counts:
            current_run += 1
            print(f"\n[{current_run}/{total_runs}] Testing {f_size // 1024} KB with {p_count} Seeder(s)...")

            # Offset ports per run to avoid Windows TIME_WAIT socket collisions
            t_port = 9000 + base_port_offset
            s_base = 9100 + base_port_offset
            d_port = 9300 + base_port_offset
            base_port_offset = (base_port_offset + 50) % 5000

            run_dir = f"bench_temp_{current_run}"
            try:
                res = run_single_test(
                    file_size_bytes=f_size,
                    num_seeders=p_count,
                    tracker_port=t_port,
                    base_seeder_port=s_base,
                    downloader_port=d_port,
                    test_run_dir=run_dir,
                )
                results.append(res)
                print(f"    -> Time: {res['elapsed_time_sec']:.3f}s | Throughput: {res['throughput_mbps']:.2f} MB/s | Chunks: {res['successful_chunks']}")
            except Exception as e:
                print(f"    -> [ERROR] Test case failed: {e}")

    df = pd.DataFrame(results)
    return df


def plot_benchmark_charts(df: pd.DataFrame, output_image: str = "benchmark_results.png") -> None:
    """
    Renders high-quality multi-panel Matplotlib charts comparing download time,
    throughput, and peer scaling efficiency.
    """
    if df.empty:
        print("[Plot] No data available to generate plots.")
        return

    plt.style.use("seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default")
    fig, axes = plt.subplots(1, 2, figsize=(14, 5.5))

    # Plot 1: Download Time vs Number of Peers
    ax1 = axes[0]
    for size_kb, group in df.groupby("file_size_kb"):
        label = f"{int(size_kb)} KB" if size_kb < 1024 else f"{size_kb / 1024:.1f} MB"
        ax1.plot(
            group["num_seeders"],
            group["elapsed_time_sec"],
            marker="o",
            linewidth=2,
            label=f"Size: {label}",
        )
    ax1.set_title("Download Time vs. Number of Seeders", fontsize=12, fontweight="bold")
    ax1.set_xlabel("Number of Seeders (Peers)", fontsize=10)
    ax1.set_ylabel("Download Time (Seconds)", fontsize=10)
    ax1.set_xticks(sorted(df["num_seeders"].unique()))
    ax1.legend()
    ax1.grid(True, linestyle="--", alpha=0.7)

    # Plot 2: Throughput (MB/s) vs Number of Peers
    ax2 = axes[1]
    for size_kb, group in df.groupby("file_size_kb"):
        label = f"{int(size_kb)} KB" if size_kb < 1024 else f"{size_kb / 1024:.1f} MB"
        ax2.plot(
            group["num_seeders"],
            group["throughput_mbps"],
            marker="s",
            linewidth=2,
            label=f"Size: {label}",
        )
    ax2.set_title("Throughput (MB/s) vs. Number of Seeders", fontsize=12, fontweight="bold")
    ax2.set_xlabel("Number of Seeders (Peers)", fontsize=10)
    ax2.set_ylabel("Throughput (MB/s)", fontsize=10)
    ax2.set_xticks(sorted(df["num_seeders"].unique()))
    ax2.legend()
    ax2.grid(True, linestyle="--", alpha=0.7)

    plt.suptitle("P2P File Distribution System - Performance Benchmarks", fontsize=14, fontweight="bold")
    plt.tight_layout()
    plt.savefig(output_image, dpi=200)
    plt.close()
    print(f"\n[Visual] Benchmark visualization chart saved successfully to: {os.path.abspath(output_image)}")


def main():
    parser = argparse.ArgumentParser(description="P2P Performance Evaluation & Benchmark")
    parser.add_argument("--quick", action="store_true", help="Run quick 6-test suite instead of full 12-test suite")
    parser.add_argument("--csv", type=str, default="benchmark_results.csv", help="CSV export destination")
    parser.add_argument("--plot", type=str, default="benchmark_results.png", help="PNG plot export destination")
    args = parser.parse_args()

    df = run_benchmark_suite(quick=args.quick)

    if not df.empty:
        # Export CSV
        df.to_csv(args.csv, index=False)
        print(f"\n[Data] Raw benchmark data exported to: {os.path.abspath(args.csv)}")

        # Print formatted summary table
        print("\n" + "=" * 80)
        print("                  AGGREGATED BENCHMARK EVALUATION RESULTS")
        print("=" * 80)
        print(df.to_string(index=False))
        print("=" * 80)

        # Generate plots
        plot_benchmark_charts(df, output_image=args.plot)
    else:
        print("[ERROR] No benchmark records generated.")


if __name__ == "__main__":
    main()
