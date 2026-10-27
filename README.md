# P2P - Peer-to-Peer File Distribution System

A complete, robust, and beginner-friendly **Peer-to-Peer (P2P) File Distribution System** built in Python using raw sockets and multi-threading. Designed specifically as a college / university computer networking and distributed systems project.

---

## 📌 Project Overview

In traditional client-server file distribution (such as HTTP or FTP), the central server acts as a bottleneck: as the number of downloading clients increases, the server's upload bandwidth saturates, degrading download speeds.

This project implements a **Hybrid P2P File Sharing Architecture** (similar to BitTorrent):
1. **Central Tracker (Directory Server)**: Facilitates peer registration, heartbeat monitoring, and file discovery. It does **not** store or transfer actual file contents.
2. **Peers (Nodes)**: Simultaneously act as **Seeders** (uploading chunks over TCP to peers) and **Leechers** (downloading chunks concurrently from multiple peers).
3. **Chunk-Level Swarming**: Files are partitioned into fixed 64 KB chunks, each validated by a SHA-256 hash. Downloader peers download chunks concurrently from multiple seeders and assemble them into the original file.
4. **Fault Tolerance**: Automatic failure detection, timeout recovery, and dynamic chunk re-queuing if a seeder disconnects mid-transfer.

---

## 🚀 Key Features

| Requirement | Implementation Detail | Source File |
| :--- | :--- | :--- |
| **1. TCP Sockets & Threading** | Non-blocking/multi-threaded TCP servers and concurrent worker pools | `src/peer.py`, `src/tracker.py` |
| **2. Peer Registration & Discovery** | Central tracker registry with automatic stale peer pruning via heartbeats | `src/tracker.py` |
| **3. TCP Protocol Framing** | Big-endian length-prefixed framing (`[4B Header Len][JSON][4B Data Len][Payload]`) | `src/protocol.py` |
| **4. Chunking & Metadata** | SHA-256 whole-file and per-chunk hashes, file metadata serialization | `src/storage.py` |
| **5. Concurrent Swarming** | Multi-threaded chunk swarming from multiple active seeders | `src/peer.py` |
| **6. Transfer Logs** | Live formatted console messages + timestamped audit CSV logs | `src/logger.py` |
| **7. Fault Tolerance** | Socket timeout detection, corrupted chunk rejection, and alternate peer failover | `src/peer.py`, `src/storage.py` |
| **8. Performance Metrics** | Measures elapsed time, throughput (KB/s, MB/s), chunk success/retry rates | `src/metrics.py` |
| **9. Benchmarking & Plots** | Benchmark suite across varying file sizes and peer counts using pandas & matplotlib | `benchmark.py` |
| **10. Python Standard + Data Libs** | `socket`, `threading`, `hashlib`, `os`, `json`, `time`, `matplotlib`, `pandas` | All modules |

---

## 📂 Project Structure

```text
P2P-File-Distribution-System/
│
├── src/                               # Core Source Code Package
│   ├── __init__.py                    # Package initializer
│   ├── config.py                      # System configuration & networking constants
│   ├── protocol.py                    # TCP length-prefixed message framing layer
│   ├── storage.py                     # File chunker, assembler, and SHA-256 engine
│   ├── tracker.py                     # Central directory server & discovery client
│   ├── peer.py                        # Peer node: TCP server, swarming downloader
│   ├── metrics.py                     # Download throughput & time measurement
│   └── logger.py                      # Console and CSV transfer logger
│
├── tests/                             # Automated Test Suite
│   ├── __init__.py
│   ├── test_protocol.py               # Unit tests for message framing & packet parsing
│   ├── test_storage.py                # Unit tests for chunking, hashes, and assembly
│   ├── test_tracker.py                # Unit tests for peer registration & discovery
│   ├── test_metrics.py                # Unit tests for throughput & statistical calculations
│   └── test_integration.py           # Integration & fault-tolerance tests
│
├── tracker_server.py                  # Standalone entry point for Central Tracker
├── peer_cli.py                        # Interactive console CLI for running peers
├── benchmark.py                       # Automated performance evaluation & plotting script
├── run_demo.py                        # One-command showcase demo (Tracker + 2 Seeders + Downloader)
│
├── run_tracker.bat                    # Windows shortcut launcher for Tracker
├── run_peer1.bat                      # Windows shortcut launcher for Peer 1 (Port 5001)
├── run_peer2.bat                      # Windows shortcut launcher for Peer 2 (Port 5002)
├── run_peer3.bat                      # Windows shortcut launcher for Peer 3 (Port 5003)
│
├── requirements.txt                   # External dependencies (matplotlib, pandas)
├── README.md                          # Main project guide and documentation
└── PROJECT_REPORT.md                  # Comprehensive academic report for submission & viva
```

---

## ⚙️ Installation & Setup (Windows)

### Step 1: Ensure Python 3.10+ is Installed
Open Command Prompt (`cmd`) or PowerShell:
```powershell
python --version
```
*(If Python is installed at a user-specific location, use its full path or reload your PATH environment variable.)*

### Step 2: Navigate to Project Directory
```powershell
cd c:\Users\KIIT\OneDrive\Documents\P2P-File-Distribution-System
```

### Step 3: Install Required Dependencies
```powershell
pip install -r requirements.txt
```
*(The core P2P file sharing engine relies purely on Python's built-in standard library: `socket`, `threading`, `hashlib`, `json`, `os`, `time`. The external packages `matplotlib` and `pandas` are used for benchmarking and plotting.)*

---

## 🏃 Execution Instructions

### Option 1: Instant Automated Showcase Demo
Run the complete end-to-end multi-peer transfer demonstration with a single command:
```powershell
python run_demo.py
```
This script automatically:
1. Starts the Central Tracker on `127.0.0.1:8000`.
2. Spins up **Peer 1** (Port 5001) and **Peer 2** (Port 5002) as Seeders holding a 1 MB file (16 chunks).
3. Spins up **Peer 3** (Port 5003) as Downloader.
4. Downloads chunks concurrently from both Peer 1 and Peer 2.
5. Verifies individual chunk SHA-256 hashes and the complete assembled file.
6. Prints a detailed performance summary table (throughput, transfer time, peer distribution).
7. Shuts down all processes cleanly.

---

### Option 2: Running Multiple Peers Locally (Manual Interactive Demo)

To demonstrate the system live for a lab evaluation or viva, open **4 separate Command Prompt or PowerShell terminals**:

#### Terminal 1: Start Central Tracker
```powershell
python tracker_server.py
```
*(Runs Tracker on `127.0.0.1:8000` to index peers and files.)*

#### Terminal 2: Start Peer 1 (Seeder A)
```powershell
python peer_cli.py --id Peer_5001 --port 5001 --shared-dir peer1_shared --downloads-dir peer1_dl
```
- Select Option `[3]` to create a dummy sample file (e.g. `presentation.pdf`, 1024 KB).
- The file is automatically chunked, hashed, and published to the Tracker.

#### Terminal 3: Start Peer 2 (Seeder B)
```powershell
python peer_cli.py --id Peer_5002 --port 5002 --shared-dir peer2_shared --downloads-dir peer2_dl
```
- Select Option `[3]` or copy a file to `peer2_shared`, then select Option `[2]` to share it.

#### Terminal 4: Start Peer 3 (Downloader)
```powershell
python peer_cli.py --id Peer_5003 --port 5003 --shared-dir peer3_shared --downloads-dir peer3_dl
```
- Select Option `[4]` to search available network files.
- Select Option `[5]` and enter the file name to download.
- Watch as chunks download concurrently from Peer 1 and Peer 2!
- Select Option `[6]` to view transfer CSV audit logs.

*(Alternatively, on Windows you can simply double-click `run_tracker.bat`, `run_peer1.bat`, `run_peer2.bat`, and `run_peer3.bat`!)*

---

### Option 3: Performance Benchmarking & Evaluation
To generate quantitative performance comparison tables and Matplotlib plots across varying file sizes and peer counts:
```powershell
python benchmark.py --quick
```
Or for full comprehensive evaluation:
```powershell
python benchmark.py
```
Outputs generated:
- `benchmark_results.csv`: Complete raw tabular metrics.
- `benchmark_results.png`: Two-panel visualization chart comparing:
  1. **Download Time vs. Number of Seeders** (per file size).
  2. **Throughput (MB/s) vs. Number of Seeders** (demonstrating swarming acceleration).

---

### Option 4: Running the Automated Test Suite
To execute the unit and integration tests:
```powershell
python -m unittest discover -s tests -v
```
All 13 test cases validate protocol framing, file chunking, hash verification, tracker discovery, and fault tolerance.

---

## 🧠 Architectural Deep-Dive

### 1. TCP Length-Prefixed Framing
TCP is a stream protocol with no inherent message boundaries. Two `send()` calls can arrive in a single `recv()` call. To ensure 100% reliable packet boundaries, all packets use binary length prefixing:
```
+---------------------------+-----------------------+--------------------------+-----------------------+
| Header Length (4B Big-End)| JSON Header (UTF-8)   | Payload Length (4B Big-E)| Raw Binary Chunk Data |
+---------------------------+-----------------------+--------------------------+-----------------------+
```

### 2. File Chunking & Integrity Protection
- Files are logically partitioned into 64 KB pieces.
- Every chunk has its own cryptographic SHA-256 hash calculated.
- When a chunk arrives at a leecher, its SHA-256 is verified before writing to staging disk. Corrupted chunks are immediately rejected and re-requested.
- Once all chunks arrive, they are concatenated in sequential order, and the whole assembled file's SHA-256 is verified against the original metadata.

### 3. Swarming & Fault Tolerance
- Download requests are distributed across active seeders in round-robin / concurrent worker threads.
- If a seeder crashes or drops the socket connection, the worker catches `ConnectionError` or `TimeoutError`, logs the failure in `transfers_<peer>.csv`, and re-queues the chunk to be retrieved from an alternate active peer.

---

## 🎓 College Viva Voce Questions & Answers

**Q1: Why is P2P file distribution more scalable than Client-Server (HTTP/FTP)?**  
*Answer:* In client-server, the server's upload capacity is shared among all downloaders; as clients increase, individual download speed drops ($O(1/N)$). In P2P, every downloading peer also uploads chunks it has already received, adding upload capacity to the network as demand grows.

**Q2: What is the role of the Tracker? Does it store file contents?**  
*Answer:* No, the Tracker does not store or relay file contents. It acts strictly as a directory service (registry), mapping file SHA-256 hashes to the IP addresses and port numbers of peers currently hosting those chunks.

**Q3: How does the system handle TCP packet fragmentation?**  
*Answer:* We implemented length-prefixed framing. Every packet starts with an 8-byte binary header containing the exact byte count of the upcoming JSON metadata and binary chunk. The receiver loops with `recv_exact()` until all bytes are collected.

**Q4: How is data integrity guaranteed against corruption or malicious peers?**  
*Answer:* Through dual-tier SHA-256 hashing. Each chunk is verified against the metadata chunk hash immediately upon download. If invalid, it is discarded. Finally, the assembled file is checked against the master file hash.

**Q5: What happens when a peer disconnects in the middle of a transfer?**  
*Answer:* Sockets use a timeout. When a connection drops or times out, the chunk worker catches the exception, records a failed transfer in the audit log, and re-inserts the chunk index into the download work queue for another active peer to fulfill.

---

## 📜 License
Developed for academic and educational project purposes. Free to use, modify, and extend.
