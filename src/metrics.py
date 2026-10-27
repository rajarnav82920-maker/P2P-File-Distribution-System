"""
Performance Metrics Collector & Analyzer.

Measures:
- Total download time
- Average throughput (KB/s, MB/s)
- Chunk success / failure / retry counts
- Multi-peer workload distribution (which peer served how many chunks)
"""

import time
from typing import Dict, Any, List


class TransferMetrics:
    """
    Collects performance statistics during a multi-peer file download.
    """

    def __init__(self, file_name: str, file_size: int, total_chunks: int):
        self.file_name = file_name
        self.file_size = file_size
        self.total_chunks = total_chunks

        self.start_time: float = 0.0
        self.end_time: float = 0.0
        self.bytes_transferred: int = 0
        self.successful_chunks: int = 0
        self.failed_chunks: int = 0
        self.retry_count: int = 0

        # Map of remote_peer_id -> {"chunks": int, "bytes": int, "time": float}
        self.peer_stats: Dict[str, Dict[str, Any]] = {}

    def start(self) -> None:
        """Marks the start of the file transfer."""
        self.start_time = time.perf_counter()

    def record_chunk_success(self, peer_id: str, chunk_size: int, duration_sec: float) -> None:
        """Records a successfully downloaded and verified chunk."""
        self.successful_chunks += 1
        self.bytes_transferred += chunk_size

        if peer_id not in self.peer_stats:
            self.peer_stats[peer_id] = {"chunks": 0, "bytes": 0, "time": 0.0}
        self.peer_stats[peer_id]["chunks"] += 1
        self.peer_stats[peer_id]["bytes"] += chunk_size
        self.peer_stats[peer_id]["time"] += duration_sec

    def record_chunk_failure(self, peer_id: str) -> None:
        """Records a failed chunk attempt."""
        self.failed_chunks += 1
        self.retry_count += 1
        if peer_id not in self.peer_stats:
            self.peer_stats[peer_id] = {"chunks": 0, "bytes": 0, "time": 0.0}

    def finish(self) -> None:
        """Marks completion of file transfer."""
        self.end_time = time.perf_counter()

    @property
    def elapsed_time(self) -> float:
        """Elapsed time in seconds."""
        if self.end_time > self.start_time:
            return self.end_time - self.start_time
        return 0.0

    @property
    def throughput_kbps(self) -> float:
        """Throughput in Kilobytes per second."""
        if self.elapsed_time > 0:
            return (self.bytes_transferred / 1024.0) / self.elapsed_time
        return 0.0

    @property
    def throughput_mbps(self) -> float:
        """Throughput in Megabytes per second."""
        return self.throughput_kbps / 1024.0

    @property
    def success_rate(self) -> float:
        """Percentage of chunk attempts that succeeded."""
        total_attempts = self.successful_chunks + self.failed_chunks
        if total_attempts > 0:
            return (self.successful_chunks / total_attempts) * 100.0
        return 100.0

    def to_dict(self) -> Dict[str, Any]:
        """Exports metrics as a dictionary for logging and pandas analysis."""
        return {
            "file_name": self.file_name,
            "file_size_bytes": self.file_size,
            "total_chunks": self.total_chunks,
            "successful_chunks": self.successful_chunks,
            "failed_chunks": self.failed_chunks,
            "retries": self.retry_count,
            "elapsed_seconds": round(self.elapsed_time, 4),
            "throughput_kbps": round(self.throughput_kbps, 2),
            "throughput_mbps": round(self.throughput_mbps, 3),
            "num_contributing_peers": len(self.peer_stats),
            "peer_distribution": {
                p: stats["chunks"] for p, stats in self.peer_stats.items()
            },
        }

    def summary(self) -> str:
        """Generates a neat ASCII report table for terminal output."""
        lines = [
            "",
            "=" * 60,
            "              DOWNLOAD PERFORMANCE SUMMARY",
            "=" * 60,
            f" File Name:            {self.file_name}",
            f" File Size:            {self.file_size:,} bytes ({self.file_size / (1024 * 1024):.2f} MB)",
            f" Total Chunks:         {self.total_chunks}",
            f" Successful Chunks:    {self.successful_chunks}",
            f" Failed / Retried:     {self.failed_chunks} (Retries: {self.retry_count})",
            f" Success Rate:         {self.success_rate:.1f}%",
            "-" * 60,
            f" Total Download Time:  {self.elapsed_time:.3f} seconds",
            f" Average Throughput:   {self.throughput_kbps:,.2f} KB/s ({self.throughput_mbps:.2f} MB/s)",
            "-" * 60,
            " Peer Workload Distribution:",
        ]

        if self.peer_stats:
            for peer_id, stats in sorted(self.peer_stats.items()):
                p_chunks = stats["chunks"]
                p_bytes = stats["bytes"]
                p_pct = (p_chunks / self.total_chunks * 100) if self.total_chunks else 0
                lines.append(
                    f"   * {peer_id:<18} -> {p_chunks:>4} chunks ({p_pct:>5.1f}%) | {p_bytes:>9,} B"
                )
        else:
            lines.append("   * None (Direct local copy or no network peers)")

        lines.append("=" * 60)
        return "\n".join(lines)
