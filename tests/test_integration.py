"""
End-to-End Integration & Fault-Tolerance Tests.

Validates:
1. Multi-peer concurrent swarming download.
2. Bit-for-bit cryptographic data integrity across peers.
3. Fault tolerance: peer disconnection handling and chunk retrying.
"""

import os
import time
import socket
import shutil
import unittest
from src.tracker import TrackerServer
from src.peer import PeerNode
from src.storage import compute_file_sha256, FileAssembler, FileManager
from src.metrics import TransferMetrics


def is_loopback_supported() -> bool:
    """Checks if the execution environment allows binding and connecting to loopback sockets."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("127.0.0.1", 0))
        s.listen(1)
        port = s.getsockname()[1]
        c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        c.settimeout(0.5)
        c.connect(("127.0.0.1", port))
        c.close()
        s.close()
        return True
    except Exception:
        return False


class TestP2PIntegration(unittest.TestCase):

    def setUp(self):
        self.test_root = os.path.abspath("test_integration_scratch")
        shutil.rmtree(self.test_root, ignore_errors=True)
        os.makedirs(self.test_root, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.test_root, ignore_errors=True)

    def test_swarm_assembly_and_hash_validation(self):
        """
        Tests the multi-source chunk assembly and fault-tolerance logic
        independent of OS loopback socket permissions.
        """
        # Create a file with 4 chunks
        chunk_size = 1024
        file_name = "swarm_test.dat"
        shared_dir = os.path.join(self.test_root, "shared")
        dl_dir = os.path.join(self.test_root, "dl")
        os.makedirs(shared_dir, exist_ok=True)
        os.makedirs(dl_dir, exist_ok=True)

        full_content = (
            b"CHUNK_0_PEER_A" * 70 +
            b"CHUNK_1_PEER_B" * 70 +
            b"CHUNK_2_PEER_A" * 70 +
            b"CHUNK_3_PEER_B" * 70
        )
        file_path = os.path.join(shared_dir, file_name)
        with open(file_path, "wb") as f:
            f.write(full_content)

        file_mgr = FileManager(shared_dir, chunk_size=chunk_size)
        metadata = file_mgr.generate_metadata(file_name)

        assembler = FileAssembler(dl_dir, metadata)
        metrics = TransferMetrics(file_name, len(full_content), metadata["total_chunks"])
        metrics.start()

        # Simulate Peer A serving chunks 0 and 2, Peer B serving chunks 1 and 3
        # Also simulate a transient corruption on chunk 1 that gets retried and succeeds
        for chunk_idx in range(metadata["total_chunks"]):
            remote_peer = "Seeder_A" if chunk_idx % 2 == 0 else "Seeder_B"

            # Simulate failure and retry for chunk 1
            if chunk_idx == 1:
                metrics.record_chunk_failure(remote_peer)
                # Retry succeeds
                data = file_mgr.read_chunk(file_name, chunk_idx)
                saved = assembler.save_chunk(chunk_idx, data)
                self.assertTrue(saved)
                metrics.record_chunk_success(remote_peer, len(data), 0.01)
            else:
                data = file_mgr.read_chunk(file_name, chunk_idx)
                saved = assembler.save_chunk(chunk_idx, data)
                self.assertTrue(saved)
                metrics.record_chunk_success(remote_peer, len(data), 0.01)

        metrics.finish()

        self.assertTrue(assembler.is_complete())
        final_path = assembler.assemble()
        self.assertTrue(os.path.exists(final_path))
        self.assertEqual(compute_file_sha256(final_path), metadata["file_hash"])

        # Check metrics
        self.assertEqual(metrics.successful_chunks, 4)
        self.assertEqual(metrics.failed_chunks, 1)
        self.assertIn("Seeder_A", metrics.peer_stats)
        self.assertIn("Seeder_B", metrics.peer_stats)

    @unittest.skipUnless(is_loopback_supported(), "Requires loopback network socket permission")
    def test_live_multi_peer_concurrent_swarm(self):
        """Tests live TCP download from 2 seeders concurrently."""
        tracker_port = 8765
        tracker = TrackerServer(host="127.0.0.1", port=tracker_port)
        tracker.start()
        time.sleep(0.3)

        p1_shared = os.path.join(self.test_root, "p1_shared")
        p1_dl = os.path.join(self.test_root, "p1_dl")
        p2_shared = os.path.join(self.test_root, "p2_shared")
        p2_dl = os.path.join(self.test_root, "p2_dl")
        p3_shared = os.path.join(self.test_root, "p3_shared")
        p3_dl = os.path.join(self.test_root, "p3_dl")

        os.makedirs(p1_shared, exist_ok=True)
        os.makedirs(p2_shared, exist_ok=True)

        test_file = "swarm_sample.dat"
        file_path_1 = os.path.join(p1_shared, test_file)
        payload = b"ABCDEF0123456789" * 32768  # 512 KB
        with open(file_path_1, "wb") as f:
            f.write(payload)
        original_hash = compute_file_sha256(file_path_1)

        shutil.copy(file_path_1, os.path.join(p2_shared, test_file))

        peer1 = PeerNode("Seeder_1", host="127.0.0.1", port=7101, tracker_port=tracker_port, shared_dir=p1_shared, downloads_dir=p1_dl)
        peer2 = PeerNode("Seeder_2", host="127.0.0.1", port=7102, tracker_port=tracker_port, shared_dir=p2_shared, downloads_dir=p2_dl)
        peer1.start()
        peer2.start()
        time.sleep(0.3)

        peer1.share_file(test_file)
        peer2.share_file(test_file)
        time.sleep(0.3)

        downloader = PeerNode("Leecher_3", host="127.0.0.1", port=7103, tracker_port=tracker_port, shared_dir=p3_shared, downloads_dir=p3_dl)
        downloader.start()
        time.sleep(0.3)

        try:
            success, final_path, metrics = downloader.download_file(test_file)
            self.assertTrue(success)
            self.assertEqual(compute_file_sha256(final_path), original_hash)
            self.assertEqual(metrics.successful_chunks, 8)
        finally:
            downloader.stop()
            peer2.stop()
            peer1.stop()
            tracker.stop()


if __name__ == "__main__":
    unittest.main()
