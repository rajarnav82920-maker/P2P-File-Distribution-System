"""
Automated Showcase Demo for P2P File Distribution System.

Executes a complete real-world scenario in one command:
1. Launches Central Tracker (Discovery Node).
2. Launches Peer 1 (Port 5001) & Peer 2 (Port 5002) as Seeding Peers.
3. Creates a sample multi-chunk payload and shares it across seeders.
4. Launches Peer 3 (Port 5003) as a Downloader.
5. Peer 3 swarms chunks concurrently from both Peer 1 and Peer 2.
6. Verifies SHA-256 hashes for each chunk and final assembled file.
7. Displays transfer statistics and generated CSV audit logs.
"""

import os
import time
import shutil
from src.tracker import TrackerServer
from src.peer import PeerNode
from src.storage import compute_file_sha256


def create_demo_file(path: str, size_kb: int = 1024) -> str:
    """Generates a demo test file with text headers and binary content."""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    header = f"=== P2P DEMO FILE: Generated for Swarm Transfer Testing ({size_kb} KB) ===\n".encode("utf-8")
    chunk_pattern = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!@#$%^&*\n" * 20

    with open(path, "wb") as f:
        f.write(header)
        written = len(header)
        target = size_kb * 1024
        while written < target:
            to_write = min(len(chunk_pattern), target - written)
            f.write(chunk_pattern[:to_write])
            written += to_write
    return path


def main():
    demo_dir = "demo_workspace"
    shutil.rmtree(demo_dir, ignore_errors=True)
    os.makedirs(demo_dir, exist_ok=True)

    print("\n" + "=" * 70)
    print("      P2P FILE DISTRIBUTION SYSTEM - LIVE DEMONSTRATION")
    print("=" * 70)

    # 1. Start Tracker Server
    print("\n[Step 1] Starting Central Tracker on 127.0.0.1:8000...")
    tracker = TrackerServer(host="127.0.0.1", port=8000)
    tracker.start()
    time.sleep(0.4)

    # 2. Setup Seeders
    print("\n[Step 2] Initializing Seeder Peers (Peer 1 on port 5001, Peer 2 on port 5002)...")
    p1_shared = os.path.join(demo_dir, "peer1_shared")
    p1_dl = os.path.join(demo_dir, "peer1_dl")
    p2_shared = os.path.join(demo_dir, "peer2_shared")
    p2_dl = os.path.join(demo_dir, "peer2_dl")

    demo_filename = "project_dataset.dat"
    file_path = os.path.join(p1_shared, demo_filename)
    create_demo_file(file_path, size_kb=1024)  # 1 MB = 16 chunks of 64 KB
    original_hash = compute_file_sha256(file_path)

    # Copy same file to Peer 2 so both peers have chunks to seed
    os.makedirs(p2_shared, exist_ok=True)
    shutil.copy(file_path, os.path.join(p2_shared, demo_filename))

    peer1 = PeerNode("Peer_5001", host="127.0.0.1", port=5001, tracker_port=8000, shared_dir=p1_shared, downloads_dir=p1_dl)
    peer2 = PeerNode("Peer_5002", host="127.0.0.1", port=5002, tracker_port=8000, shared_dir=p2_shared, downloads_dir=p2_dl)

    peer1.start()
    peer2.start()
    time.sleep(0.5)

    # 3. Share File
    print(f"\n[Step 3] Publishing '{demo_filename}' (SHA-256: {original_hash[:16]}...) to Tracker...")
    peer1.share_file(demo_filename)
    peer2.share_file(demo_filename)
    time.sleep(0.3)

    # 4. Start Downloader
    print("\n[Step 4] Starting Downloader (Peer 3 on port 5003)...")
    p3_shared = os.path.join(demo_dir, "peer3_shared")
    p3_dl = os.path.join(demo_dir, "peer3_dl")
    peer3 = PeerNode("Peer_5003", host="127.0.0.1", port=5003, tracker_port=8000, shared_dir=p3_shared, downloads_dir=p3_dl)
    peer3.start()
    time.sleep(0.3)

    # 5. Query network files from Downloader
    print("\n[Step 5] Peer 3 querying files from network Tracker:")
    available = peer3.tracker.list_files()
    for f in available:
        print(f"   -> Found '{f['file_name']}' | {f['file_size']:,} B | {f['total_chunks']} chunks | Seeders: {f['seeders_count']}")

    # 6. Execute Multi-Peer Swarm Download
    print(f"\n[Step 6] Peer 3 initiating multi-source download of '{demo_filename}'...")
    success, downloaded_path, metrics = peer3.download_file(demo_filename)

    # 7. Verification & Summary
    print("\n[Step 7] Validating Downloaded Integrity:")
    if success and downloaded_path and os.path.exists(downloaded_path):
        dl_hash = compute_file_sha256(downloaded_path)
        print(f"   Original File SHA-256   : {original_hash}")
        print(f"   Downloaded File SHA-256 : {dl_hash}")
        if original_hash == dl_hash:
            print("   -> RESULT: PERFECT MATCH! All chunks verified cryptographically.")
        else:
            print("   -> RESULT: HASH MISMATCH! Corruption detected.")

        if metrics:
            print(metrics.summary())
    else:
        print("   -> RESULT: Download failed!")

    # 8. Clean up
    print("\n[Step 8] Graceful Shutdown of Nodes...")
    peer3.stop()
    peer2.stop()
    peer1.stop()
    tracker.stop()

    print("\n" + "=" * 70)
    print("           DEMO COMPLETED SUCCESSFULLY")
    print("=" * 70)


if __name__ == "__main__":
    main()
