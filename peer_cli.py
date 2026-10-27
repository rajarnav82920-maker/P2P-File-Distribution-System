"""
Interactive Command-Line Interface (CLI) for P2P Peer Node.

Features:
- Share local files and publish chunk metadata
- Search available files across all peers
- Download files concurrently from multiple peers (swarming)
- Live transfer progress and statistics
- View transfer history from CSV logs
"""

import os
import sys
import time
import argparse
from typing import Optional

from src.peer import PeerNode
from src.config import (
    DEFAULT_PEER_HOST,
    DEFAULT_PEER_PORT,
    DEFAULT_TRACKER_HOST,
    DEFAULT_TRACKER_PORT,
)


def print_banner(peer_id: str, host: str, port: int, shared_dir: str, dl_dir: str):
    print("\n" + "=" * 68)
    print("        P2P FILE DISTRIBUTION SYSTEM - PEER CONSOLE")
    print("=" * 68)
    print(f" Peer ID       : {peer_id}")
    print(f" Address       : {host}:{port}")
    print(f" Shared Folder : {shared_dir}")
    print(f" Downloads Dir : {dl_dir}")
    print("=" * 68)


def print_menu():
    print("""
Available Operations:
  [1] List My Shared Files
  [2] Share a File from Shared Folder
  [3] Create Sample Dummy File to Share
  [4] Discover Files on Network (from Tracker)
  [5] Download a File from Swarm (Multi-Peer)
  [6] View Transfer Statistics & CSV Logs
  [7] Check Network Seeders for a File
  [8] Exit Peer
""")


def handle_share_file(peer: PeerNode):
    files = [f for f in os.listdir(peer.shared_dir) if os.path.isfile(os.path.join(peer.shared_dir, f))]
    if not files:
        print(f"No files found in shared folder '{peer.shared_dir}'.")
        print("Tip: Use option [3] to create a sample dummy file!")
        return

    print("\nFiles in your shared folder:")
    for idx, fname in enumerate(files, 1):
        fsize = os.path.getsize(os.path.join(peer.shared_dir, fname))
        print(f"  {idx}. {fname} ({fsize:,} bytes)")

    choice = input("\nEnter file name or number to share (or press Enter to cancel): ").strip()
    if not choice:
        return

    if choice.isdigit() and 1 <= int(choice) <= len(files):
        target_file = files[int(choice) - 1]
    else:
        target_file = choice

    try:
        meta = peer.share_file(target_file)
        print(f"\n[SUCCESS] '{target_file}' is now actively shared on the network!")
        print(f"Total Chunks: {meta['total_chunks']} | SHA-256: {meta['file_hash'][:16]}...")
    except Exception as e:
        print(f"\n[ERROR] Failed to share file: {e}")


def handle_create_dummy(peer: PeerNode):
    name = input("Enter dummy file name (default: sample_data.txt): ").strip() or "sample_data.txt"
    size_kb_str = input("Enter file size in KB (default: 512): ").strip() or "512"
    try:
        size_kb = int(size_kb_str)
        target_path = os.path.join(peer.shared_dir, name)
        # Create patterned binary data
        block = b"Antigravity P2P File Distribution System Demo Chunk Line\n" * 16
        with open(target_path, "wb") as f:
            written = 0
            target_bytes = size_kb * 1024
            while written < target_bytes:
                to_write = min(len(block), target_bytes - written)
                f.write(block[:to_write])
                written += to_write

        print(f"\n[SUCCESS] Created dummy file '{name}' ({size_kb} KB) in '{peer.shared_dir}'.")
        peer.share_file(name)
    except Exception as e:
        print(f"\n[ERROR] Could not create file: {e}")


def handle_list_network_files(peer: PeerNode):
    try:
        files = peer.tracker.list_files()
        if not files:
            print("\nNo files currently registered on the network.")
            return

        print("\n" + "-" * 72)
        print(f"{'#':<3} {'File Name':<28} {'Size (B)':<12} {'Chunks':<8} {'Seeders':<8}")
        print("-" * 72)
        for idx, f in enumerate(files, 1):
            print(f"{idx:<3} {f['file_name']:<28} {f['file_size']:<12,} {f['total_chunks']:<8} {f['seeders_count']:<8}")
        print("-" * 72)
    except Exception as e:
        print(f"\n[ERROR] Failed to reach tracker: {e}")


def handle_download_file(peer: PeerNode):
    filename = input("Enter exact file name to download: ").strip()
    if not filename:
        return

    print(f"\n[SWARM] Starting concurrent download for '{filename}'...")
    success, file_path, metrics = peer.download_file(filename)

    if success and metrics:
        print(metrics.summary())
    else:
        print(f"\n[ERROR] Download failed or file not found on network.")


def handle_view_logs(peer: PeerNode):
    csv_file = peer.logger.csv_path
    if not os.path.exists(csv_file):
        print(f"\nNo transfer logs recorded yet at: {csv_file}")
        return

    print(f"\nRecent transfers from CSV: {csv_file}")
    print("-" * 75)
    with open(csv_file, "r", encoding="utf-8") as f:
        lines = f.readlines()
        for line in lines[-15:]:  # show up to last 15 entries
            print(line.strip())
    print("-" * 75)


def handle_check_seeders(peer: PeerNode):
    filename = input("Enter file name to query seeders: ").strip()
    if not filename:
        return

    files = peer.tracker.list_files()
    match = next((f for f in files if f["file_name"] == filename), None)
    if not match:
        print(f"File '{filename}' not found on Tracker.")
        return

    _, seeders = peer.tracker.get_file_peers(match["file_hash"])
    print(f"\nSeeders offering '{filename}' ({len(seeders)} active):")
    for s in seeders:
        print(f"  - Peer ID: {s['peer_id']:<15} Host: {s['host']}:{s['port']}")


def main():
    parser = argparse.ArgumentParser(description="P2P File Distribution System - Peer Node")
    parser.add_argument("--id", type=str, default=None, help="Unique Peer ID (e.g. Peer_5001)")
    parser.add_argument("--host", type=str, default=DEFAULT_PEER_HOST, help="Peer Host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=DEFAULT_PEER_PORT, help="Peer Port (default: 5001)")
    parser.add_argument("--tracker-host", type=str, default=DEFAULT_TRACKER_HOST, help="Tracker Host")
    parser.add_argument("--tracker-port", type=int, default=DEFAULT_TRACKER_PORT, help="Tracker Port")
    parser.add_argument("--shared-dir", type=str, default=None, help="Directory containing files to share")
    parser.add_argument("--downloads-dir", type=str, default=None, help="Directory to save downloaded files")
    parser.add_argument("--share", type=str, default=None, help="Non-interactive: Share this file and keep running")
    parser.add_argument("--download", type=str, default=None, help="Non-interactive: Download this file and exit")
    args = parser.parse_args()

    port = args.port
    peer_id = args.id or f"Peer_{port}"
    shared_dir = args.shared_dir or f"peer_{port}_shared"
    downloads_dir = args.downloads_dir or f"peer_{port}_downloads"

    peer = PeerNode(
        peer_id=peer_id,
        host=args.host,
        port=port,
        tracker_host=args.tracker_host,
        tracker_port=args.tracker_port,
        shared_dir=shared_dir,
        downloads_dir=downloads_dir,
    )

    try:
        peer.start()
    except OSError as e:
        print(f"[FATAL] Cannot start peer on port {port}: {e}")
        return

    # Non-interactive CLI command execution
    if args.share:
        file_to_share = args.share
        if not os.path.exists(os.path.join(peer.shared_dir, file_to_share)):
            # If path was given directly, copy it to shared_dir
            if os.path.exists(file_to_share):
                import shutil
                shutil.copy(file_to_share, os.path.join(peer.shared_dir, os.path.basename(file_to_share)))
                file_to_share = os.path.basename(file_to_share)
        peer.share_file(file_to_share)

    if args.download:
        print(f"[{peer_id}] Commencing automated download of '{args.download}'...")
        success, final_path, metrics = peer.download_file(args.download)
        if success and metrics:
            print(metrics.summary())
            peer.stop()
            sys.exit(0)
        else:
            print(f"[{peer_id}] Download failed.")
            peer.stop()
            sys.exit(1)

    print_banner(peer_id, args.host, port, shared_dir, downloads_dir)

    try:
        while peer.running:
            print_menu()
            choice = input("Select an option [1-8]: ").strip()

            if choice == "1":
                files = [f for f in os.listdir(peer.shared_dir) if os.path.isfile(os.path.join(peer.shared_dir, f))]
                print(f"\nFiles in '{peer.shared_dir}':")
                if not files:
                    print("  (Folder is empty)")
                for f in files:
                    size = os.path.getsize(os.path.join(peer.shared_dir, f))
                    print(f"  - {f} ({size:,} bytes)")

            elif choice == "2":
                handle_share_file(peer)

            elif choice == "3":
                handle_create_dummy(peer)

            elif choice == "4":
                handle_list_network_files(peer)

            elif choice == "5":
                handle_download_file(peer)

            elif choice == "6":
                handle_view_logs(peer)

            elif choice == "7":
                handle_check_seeders(peer)

            elif choice == "8":
                print("\nExiting peer console...")
                break
            else:
                print("Invalid choice, please select between 1 and 8.")

    except KeyboardInterrupt:
        print("\nPeer shutdown initiated...")
    finally:
        peer.stop()


if __name__ == "__main__":
    main()
