"""
Unit Tests for Metrics Collector and Throughput Calculation.
"""

import time
import unittest
from src.metrics import TransferMetrics


class TestMetrics(unittest.TestCase):

    def test_metrics_calculation(self):
        metrics = TransferMetrics("test_file.bin", file_size=1048576, total_chunks=16)
        metrics.start()
        time.sleep(0.05)  # small delta

        # Record 15 successes and 1 failure
        for i in range(15):
            peer = "Peer_A" if i % 2 == 0 else "Peer_B"
            metrics.record_chunk_success(peer, 65536, duration_sec=0.003)

        metrics.record_chunk_failure("Peer_A")
        metrics.finish()

        self.assertEqual(metrics.successful_chunks, 15)
        self.assertEqual(metrics.failed_chunks, 1)
        self.assertEqual(metrics.retry_count, 1)
        self.assertGreater(metrics.elapsed_time, 0)
        self.assertGreater(metrics.throughput_kbps, 0)
        self.assertGreater(metrics.throughput_mbps, 0)

        # Check export dict
        data = metrics.to_dict()
        self.assertEqual(data["file_name"], "test_file.bin")
        self.assertEqual(data["total_chunks"], 16)
        self.assertEqual(data["successful_chunks"], 15)
        self.assertEqual(data["num_contributing_peers"], 2)

        # Check summary formatting
        summary = metrics.summary()
        self.assertIn("DOWNLOAD PERFORMANCE SUMMARY", summary)
        self.assertIn("Peer_A", summary)
        self.assertIn("Peer_B", summary)


if __name__ == "__main__":
    unittest.main()
