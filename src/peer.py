"""
Peer Node Core Engine.

Combines:
1. TCP Server (Seeder / Upload service): Serves metadata and file chunks.
2. Swarming Downloader (Leecher): Concurrently downloads chunks from multiple peers.
3. Tracker Client: Registers node, advertises shared files, discovers network sources.
4. Fault Tolerance: Retries dropped chunks on alternative peers when peers disconnect.
5. Metrics & Transfer Logging: Records real-time performance and audit CSVs.
"""

import os
import time
import queue
import socket
import threading
from typing import Dict, List, Any, Optional, Tuple

from src.config import (
    DEFAULT_PEER_HOST,
    DEFAULT_PEER_PORT,
    DEFAULT_TRACKER_HOST,
    DEFAULT_TRACKER_PORT,
    DEFAULT_CHUNK_SIZE,
    SOCKET_TIMEOUT,
    PEER_HEARTBEAT_INTERVAL,
    MAX_CONCURRENT_DOWNLOADS,
    MAX_CHUNK_RETRIES,
)
from src.protocol import (
    MSG_GET_METADATA,
    MSG_METADATA_RESPONSE,
    MSG_GET_CHUNK,
    MSG_CHUNK_RESPONSE,
    MSG_PING,
    MSG_PONG,
    MSG_ERROR,
    send_message,
    recv_message,
)
from src.storage import FileManager, FileAssembler, compute_sha256
from src.tracker import TrackerClient
from src.logger import TransferLogger
from src.metrics import TransferMetrics


class PeerNode:
    """
    A full Peer in the P2P network, capable of both seeding (uploading)
    and swarming (concurrent downloading).
    """

    def __init__(
        self,
        peer_id: str,
        host: str = DEFAULT_PEER_HOST,
        port: int = DEFAULT_PEER_PORT,
        tracker_host: str = DEFAULT_TRACKER_HOST,
        tracker_port: int = DEFAULT_TRACKER_PORT,
        shared_dir: str = "shared",
        downloads_dir: str = "downloads",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ):
        self.peer_id = peer_id
        self.host = host
        self.port = port
        self.chunk_size = chunk_size

        # Directories
        self.shared_dir = os.path.abspath(shared_dir)
        self.downloads_dir = os.path.abspath(downloads_dir)
        os.makedirs(self.shared_dir, exist_ok=True)
        os.makedirs(self.downloads_dir, exist_ok=True)

        # File managers
        self.storage = FileManager(self.shared_dir, chunk_size=chunk_size)
        self.logger = TransferLogger(log_dir=os.path.join(self.shared_dir, "..", "logs"), peer_id=self.peer_id)
        self.tracker = TrackerClient(tracker_host, tracker_port)

        # Local metadata cache: file_hash -> metadata dict
        self.local_files: Dict[str, Dict[str, Any]] = {}

        # Networking state
        self.server_socket: Optional[socket.socket] = None
        self.running = False
        self._server_thread: Optional[threading.Thread] = None
        self._heartbeat_thread: Optional[threading.Thread] = None

    # =========================================================================
    # Lifecycle: Start, Heartbeat, and Stop
    # =========================================================================

    def start(self) -> None:
        """
        Starts the peer's TCP server socket and registers with the Tracker.
        """
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(50)
        self.running = True

        # 1. Start TCP server listener thread for incoming chunk requests
        self._server_thread = threading.Thread(target=self._listen_loop, daemon=True)
        self._server_thread.start()

        # 2. Register peer with central tracker
        try:
            reg_resp = self.tracker.register_peer(self.peer_id, self.port, self.host)
            print(f"[{self.peer_id}] Registered with tracker: {reg_resp.get('status')}")
        except Exception as e:
            print(f"[{self.peer_id}] Warning: Tracker offline or registration failed: {e}")

        # 3. Start background heartbeat thread
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()

        # 4. Auto-scan existing files in shared directory
        self.scan_and_register_shared_files()

        print(f"[{self.peer_id}] Peer online at {self.host}:{self.port} | Shared dir: {self.shared_dir}")

    def stop(self) -> None:
        """
        Gracefully shuts down peer server, unregisters from tracker, and releases resources.
        """
        if not self.running:
            return

        self.running = False
        try:
            self.tracker.unregister_peer(self.peer_id)
        except Exception:
            pass

        if self.server_socket:
            try:
                self.server_socket.close()
            except OSError:
                pass
            self.server_socket = None

        print(f"[{self.peer_id}] Peer stopped.")

    def _heartbeat_loop(self) -> None:
        """Sends periodic keep-alive heartbeats to the Tracker."""
        while self.running:
            time.sleep(PEER_HEARTBEAT_INTERVAL)
            if not self.running:
                break
            try:
                self.tracker.heartbeat(self.peer_id)
            except Exception:
                pass

    # =========================================================================
    # Seeder Service: Handling Incoming Upload Requests (TCP Server)
    # =========================================================================

    def _listen_loop(self) -> None:
        """Listens for and accepts incoming peer connections."""
        while self.running:
            try:
                client_sock, client_addr = self.server_socket.accept()
                client_sock.settimeout(SOCKET_TIMEOUT)
                handler = threading.Thread(
                    target=self._handle_incoming_connection,
                    args=(client_sock, client_addr),
                    daemon=True,
                )
                handler.start()
            except OSError:
                break

    def _handle_incoming_connection(self, sock: socket.socket, addr: tuple) -> None:
        """Processes requests from a remote peer (metadata query or chunk request)."""
        remote_name = f"{addr[0]}:{addr[1]}"
        try:
            while self.running:
                try:
                    header, _ = recv_message(sock)
                except (ConnectionError, TimeoutError):
                    break

                msg_type = header.get("type")

                if msg_type == MSG_PING:
                    send_message(sock, {"type": MSG_PONG, "peer_id": self.peer_id})

                elif msg_type == MSG_GET_METADATA:
                    filename = header.get("file_name")
                    file_hash = header.get("file_hash")

                    metadata = None
                    if file_hash and file_hash in self.local_files:
                        metadata = self.local_files[file_hash]
                    elif filename:
                        # Check shared dir or downloads dir
                        if os.path.exists(os.path.join(self.shared_dir, filename)):
                            metadata = self.storage.generate_metadata(filename)
                        elif os.path.exists(os.path.join(self.downloads_dir, filename)):
                            dl_storage = FileManager(self.downloads_dir, self.chunk_size)
                            metadata = dl_storage.generate_metadata(filename)

                    if metadata:
                        send_message(sock, {"type": MSG_METADATA_RESPONSE, "status": "OK", "metadata": metadata})
                    else:
                        send_message(sock, {"type": MSG_ERROR, "message": f"File '{filename}' not found"})

                elif msg_type == MSG_GET_CHUNK:
                    req_peer = header.get("requester_id", remote_name)
                    filename = header.get("file_name")
                    chunk_index = header.get("chunk_index")
                    t0 = time.perf_counter()

                    try:
                        # Attempt to read chunk from shared folder first, then downloads folder
                        chunk_data = b""
                        if os.path.exists(os.path.join(self.shared_dir, filename)):
                            chunk_data = self.storage.read_chunk(filename, chunk_index)
                        elif os.path.exists(os.path.join(self.downloads_dir, filename)):
                            dl_storage = FileManager(self.downloads_dir, self.chunk_size)
                            chunk_data = dl_storage.read_chunk(filename, chunk_index)
                        else:
                            raise FileNotFoundError(f"File {filename} not located")

                        duration = time.perf_counter() - t0
                        resp_header = {
                            "type": MSG_CHUNK_RESPONSE,
                            "status": "OK",
                            "file_name": filename,
                            "chunk_index": chunk_index,
                            "chunk_size": len(chunk_data),
                            "chunk_hash": compute_sha256(chunk_data),
                        }
                        send_message(sock, resp_header, chunk_data)

                        # Log successful upload
                        self.logger.log_event(
                            direction="UPLOAD",
                            remote_peer=req_peer,
                            file_name=filename,
                            chunk_index=chunk_index,
                            bytes_transferred=len(chunk_data),
                            duration_sec=duration,
                            status="SUCCESS",
                        )

                    except Exception as e:
                        duration = time.perf_counter() - t0
                        send_message(sock, {
                            "type": MSG_ERROR,
                            "message": f"Failed reading chunk #{chunk_index}: {e}",
                        })
                        self.logger.log_event(
                            direction="UPLOAD",
                            remote_peer=req_peer,
                            file_name=filename,
                            chunk_index=chunk_index,
                            bytes_transferred=0,
                            duration_sec=duration,
                            status="FAILED",
                            error_message=str(e),
                        )
                else:
                    send_message(sock, {"type": MSG_ERROR, "message": f"Unknown type: {msg_type}"})

        except Exception:
            pass
        finally:
            sock.close()

    # =========================================================================
    # File Sharing & Registration
    # =========================================================================

    def share_file(self, filename: str) -> Dict[str, Any]:
        """
        Prepares a local file in shared_dir for sharing:
        Calculates SHA-256 chunk hashes and announces it to the Tracker.
        """
        metadata = self.storage.generate_metadata(filename)
        file_hash = metadata["file_hash"]
        self.local_files[file_hash] = metadata

        # Register file with central tracker
        try:
            self.tracker.register_file(self.peer_id, metadata)
            print(f"[{self.peer_id}] Shared '{filename}' ({metadata['total_chunks']} chunks, {metadata['file_size']} bytes)")
        except Exception as e:
            print(f"[{self.peer_id}] Registered locally, but Tracker error: {e}")

        return metadata

    def scan_and_register_shared_files(self) -> None:
        """Scans shared_dir and registers all existing files."""
        for fname in os.listdir(self.shared_dir):
            fpath = os.path.join(self.shared_dir, fname)
            if os.path.isfile(fpath) and not fname.startswith("."):
                try:
                    self.share_file(fname)
                except Exception as e:
                    print(f"[{self.peer_id}] Could not auto-share {fname}: {e}")

    # =========================================================================
    # Concurrent Multi-Source Downloader (Swarm Engine)
    # =========================================================================

    def _fetch_metadata_from_peer(self, peer_host: str, peer_port: int, filename: str) -> Optional[Dict[str, Any]]:
        """Queries a remote peer directly for file metadata."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(SOCKET_TIMEOUT)
        try:
            sock.connect((peer_host, peer_port))
            send_message(sock, {"type": MSG_GET_METADATA, "file_name": filename})
            header, _ = recv_message(sock)
            if header.get("status") == "OK":
                return header.get("metadata")
            return None
        except Exception:
            return None
        finally:
            sock.close()

    def _download_chunk_from_peer(
        self,
        peer_info: Dict[str, Any],
        filename: str,
        chunk_index: int,
    ) -> Tuple[bool, bytes, str]:
        """
        Connects via TCP to a specific peer and requests chunk_index.
        Returns: (success: bool, chunk_bytes: bytes, error_message: str)
        """
        host = peer_info["host"]
        port = peer_info["port"]
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(SOCKET_TIMEOUT)

        try:
            sock.connect((host, port))
            request = {
                "type": MSG_GET_CHUNK,
                "requester_id": self.peer_id,
                "file_name": filename,
                "chunk_index": chunk_index,
            }
            send_message(sock, request)
            header, payload = recv_message(sock)

            if header.get("status") == "OK" and header.get("chunk_index") == chunk_index:
                return True, payload, ""
            else:
                return False, b"", header.get("message", "Non-OK status from peer")
        except Exception as e:
            return False, b"", str(e)
        finally:
            sock.close()

    def download_file(
        self,
        filename: str,
        file_hash: Optional[str] = None,
        max_workers: int = MAX_CONCURRENT_DOWNLOADS,
    ) -> Tuple[bool, Optional[str], Optional[TransferMetrics]]:
        """
        High-Performance Concurrent Multi-Source Download.

        Steps:
        1. Discover active seeders from Tracker.
        2. Retrieve file metadata (chunk hashes, total chunks).
        3. Partition chunk download queue across available peers concurrently.
        4. Validate every chunk with SHA-256 upon arrival.
        5. Automatically retry failed chunks on another peer.
        6. Reconstruct the complete file in downloads_dir.
        7. Announce completed file to Tracker as a new seeder.

        Returns: (success: bool, final_file_path: str, metrics: TransferMetrics)
        """
        # Step 1: Discover peers from Tracker
        metadata = None
        seeders = []

        if file_hash:
            metadata, seeders = self.tracker.get_file_peers(file_hash)
        else:
            # Query tracker file list to find file_hash by filename
            available = self.tracker.list_files()
            match = next((f for f in available if f["file_name"] == filename), None)
            if match:
                file_hash = match["file_hash"]
                metadata, seeders = self.tracker.get_file_peers(file_hash)

        # Filter out self from seeders (avoid requesting from self)
        seeders = [p for p in seeders if p.get("peer_id") != self.peer_id]

        if not seeders:
            print(f"[{self.peer_id}] Error: No available peers found offering '{filename}'")
            return False, None, None

        # Step 2: Fetch metadata if not provided by Tracker
        if not metadata:
            for seeder in seeders:
                metadata = self._fetch_metadata_from_peer(seeder["host"], seeder["port"], filename)
                if metadata:
                    break

        if not metadata:
            print(f"[{self.peer_id}] Error: Failed to retrieve metadata for '{filename}'")
            return False, None, None

        total_chunks = metadata["total_chunks"]
        file_size = metadata["file_size"]
        assembler = FileAssembler(self.downloads_dir, metadata)

        # Initialize metrics collector
        metrics = TransferMetrics(filename, file_size, total_chunks)
        metrics.start()

        # Check if already complete
        if assembler.is_complete():
            assembled_path = assembler.assemble()
            metrics.finish()
            return True, assembled_path, metrics

        missing_chunks = assembler.get_missing_chunks()
        chunk_queue: queue.Queue = queue.Queue()
        for idx in missing_chunks:
            chunk_queue.put(idx)

        # Track retries per chunk to avoid infinite loops
        chunk_retries: Dict[int, int] = {idx: 0 for idx in missing_chunks}
        chunk_retries_lock = threading.Lock()

        # Shared peer round-robin pointer
        peer_lock = threading.Lock()
        peer_cursor = 0

        # Worker pool
        num_workers = min(max_workers, len(seeders) * 2, max(1, len(missing_chunks)))
        threads: List[threading.Thread] = []

        def worker_loop():
            nonlocal peer_cursor
            while not chunk_queue.empty() and self.running:
                try:
                    chunk_idx = chunk_queue.get_nowait()
                except queue.Empty:
                    break

                # Pick a peer using round-robin distribution
                with peer_lock:
                    target_peer = seeders[peer_cursor % len(seeders)]
                    peer_cursor += 1

                t_start = time.perf_counter()
                success, data, err = self._download_chunk_from_peer(target_peer, filename, chunk_idx)
                duration = time.perf_counter() - t_start

                if success and assembler.save_chunk(chunk_idx, data):
                    # Chunk successfully downloaded and SHA-256 verified!
                    metrics.record_chunk_success(target_peer["peer_id"], len(data), duration)
                    self.logger.log_event(
                        direction="DOWNLOAD",
                        remote_peer=target_peer["peer_id"],
                        file_name=filename,
                        chunk_index=chunk_idx,
                        bytes_transferred=len(data),
                        duration_sec=duration,
                        status="SUCCESS",
                    )
                    chunk_queue.task_done()
                else:
                    # Download failed or SHA-256 mismatch!
                    metrics.record_chunk_failure(target_peer["peer_id"])
                    error_msg = err if not success else "SHA-256 chunk hash mismatch"
                    self.logger.log_event(
                        direction="DOWNLOAD",
                        remote_peer=target_peer["peer_id"],
                        file_name=filename,
                        chunk_index=chunk_idx,
                        bytes_transferred=0,
                        duration_sec=duration,
                        status="FAILED",
                        error_message=error_msg,
                    )

                    with chunk_retries_lock:
                        chunk_retries[chunk_idx] += 1
                        if chunk_retries[chunk_idx] <= MAX_CHUNK_RETRIES:
                            print(f"[{self.peer_id}] Retrying chunk #{chunk_idx} (Attempt {chunk_retries[chunk_idx]}/{MAX_CHUNK_RETRIES})")
                            chunk_queue.put(chunk_idx)
                        else:
                            print(f"[{self.peer_id}] Max retries exceeded for chunk #{chunk_idx}")

                    chunk_queue.task_done()

        # Launch download worker threads
        for _ in range(num_workers):
            t = threading.Thread(target=worker_loop, daemon=True)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        metrics.finish()

        # Step 6: Assemble downloaded chunks
        if assembler.is_complete():
            final_path = assembler.assemble()
            print(f"[{self.peer_id}] File '{filename}' successfully assembled at: {final_path}")

            # Step 7: Swarm seeding - register downloaded file with tracker as new seeder
            self.local_files[metadata["file_hash"]] = metadata
            try:
                self.tracker.register_file(self.peer_id, metadata)
                print(f"[{self.peer_id}] Joined swarm as a seeder for '{filename}'")
            except Exception:
                pass

            return True, final_path, metrics
        else:
            missing = assembler.get_missing_chunks()
            print(f"[{self.peer_id}] Transfer incomplete. Missing chunks: {missing}")
            return False, None, metrics
