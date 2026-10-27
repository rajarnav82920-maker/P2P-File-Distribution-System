"""
Central Tracker Server Executable.

Run this script first to launch the network's Discovery & Directory Server.
Usage:
    python tracker_server.py [--host 127.0.0.1] [--port 8000]
"""

import sys
import argparse
from src.tracker import TrackerServer
from src.config import DEFAULT_TRACKER_HOST, DEFAULT_TRACKER_PORT


def main():
    parser = argparse.ArgumentParser(description="P2P Central Tracker & Discovery Server")
    parser.add_argument("--host", type=str, default=DEFAULT_TRACKER_HOST, help="Tracker host IP (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=DEFAULT_TRACKER_PORT, help="Tracker port (default: 8000)")
    args = parser.parse_args()

    print("=" * 65)
    print("      P2P FILE DISTRIBUTION SYSTEM - CENTRAL TRACKER")
    print("=" * 65)
    print(f" Binding Address  : {args.host}:{args.port}")
    print(" Roles            : Peer Discovery, File Indexing, Swarm Coordination")
    print(" Status           : Starting server...")
    print(" Press Ctrl+C to terminate.")
    print("=" * 65)

    server = TrackerServer(host=args.host, port=args.port)
    try:
        server.run_forever()
    except KeyboardInterrupt:
        print("\n[Tracker] Shutdown signal received. Exiting...")
    finally:
        server.stop()


if __name__ == "__main__":
    main()
