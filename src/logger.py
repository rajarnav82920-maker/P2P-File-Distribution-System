"""
Transfer Logging & Console Output Formatting.

Logs all upload and download activity to both:
1. Formatted readable stdout console.
2. Structured CSV files for audit, evaluation, and viva verification.
"""

import os
import csv
import time
import threading
from datetime import datetime
from typing import Optional


class TransferLogger:
    """
    Thread-safe logger recording upload/download transfer events to console & CSV.
    """

    def __init__(self, log_dir: str = "logs", peer_id: str = "peer"):
        self.log_dir = os.path.abspath(log_dir)
        self.peer_id = peer_id
        os.makedirs(self.log_dir, exist_ok=True)

        self.csv_path = os.path.join(self.log_dir, f"transfers_{self.peer_id}.csv")
        self.lock = threading.Lock()
        self._init_csv()

    def _init_csv(self) -> None:
        """Initializes the CSV log file header if not already present."""
        with self.lock:
            if not os.path.exists(self.csv_path):
                with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        "timestamp",
                        "direction",
                        "local_peer",
                        "remote_peer",
                        "file_name",
                        "chunk_index",
                        "bytes_transferred",
                        "duration_sec",
                        "throughput_kbps",
                        "status",
                        "error_message",
                    ])

    def log_event(
        self,
        direction: str,
        remote_peer: str,
        file_name: str,
        chunk_index: int,
        bytes_transferred: int,
        duration_sec: float,
        status: str = "SUCCESS",
        error_message: str = "",
    ) -> None:
        """
        Logs a single chunk transfer event (upload or download).
        """
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        kbps = (bytes_transferred / 1024.0) / duration_sec if duration_sec > 0 else 0.0

        # 1. Thread-safe write to CSV
        with self.lock:
            with open(self.csv_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([
                    now_str,
                    direction,
                    self.peer_id,
                    remote_peer,
                    file_name,
                    chunk_index,
                    bytes_transferred,
                    f"{duration_sec:.4f}",
                    f"{kbps:.2f}",
                    status,
                    error_message,
                ])

        # 2. Formatted terminal message
        symbol = "^" if direction.upper() == "UPLOAD" else "v"
        color_tag = f"[{direction.upper()}]"
        status_tag = f"[{status}]" if status == "SUCCESS" else f"[{status}: {error_message}]"

        log_line = (
            f"{now_str} {symbol} {color_tag:<10} File: {file_name} | Chunk #{chunk_index} "
            f"({bytes_transferred} B) | Remote: {remote_peer} | {kbps:.1f} KB/s {status_tag}"
        )
        try:
            print(log_line)
        except UnicodeEncodeError:
            print(log_line.encode("ascii", errors="replace").decode("ascii"))
