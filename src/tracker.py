"""
Central Tracker & Peer Discovery Server.

The Tracker serves as a directory server for the P2P network:
1. Maintains an active registry of peers (IP, port, last seen timestamp).
2. Indexes files and maps file hashes to peers holding those files (seeders).
3. Automatically prunes stale/disconnected peers via heartbeat monitoring.
4. Provides discovery endpoints for peers to locate chunk sources.
"""

import time
import socket
import threading
from typing import Dict, List, Any, Optional, Tuple

from src.config import (
    DEFAULT_TRACKER_HOST,
    DEFAULT_TRACKER_PORT,
    TRACKER_STALE_TIMEOUT,
    SOCKET_TIMEOUT,
)
from src.protocol import (
    MSG_REGISTER_PEER,
    MSG_REGISTER_PEER_ACK,
    MSG_UNREGISTER_PEER,
    MSG_UNREGISTER_PEER_ACK,
    MSG_REGISTER_FILE,
    MSG_REGISTER_FILE_ACK,
    MSG_LIST_FILES,
    MSG_LIST_FILES_ACK,
    MSG_GET_FILE_PEERS,
    MSG_GET_FILE_PEERS_ACK,
    MSG_HEARTBEAT,
    MSG_HEARTBEAT_ACK,
    MSG_ERROR,
    send_message,
    recv_message,
)


class TrackerServer:
    """
    Threaded TCP Tracker Server for peer discovery and file indexing.
    """

    def __init__(self, host: str = DEFAULT_TRACKER_HOST, port: int = DEFAULT_TRACKER_PORT):
        self.host = host
        self.port = port
        self.running = False
        self.server_socket: Optional[socket.socket] = None
        self.lock = threading.Lock()

        # In-memory registries:
        # peers: peer_id -> {"host": str, "port": int, "last_seen": float, "files": set(file_hashes)}
        self.peers: Dict[str, Dict[str, Any]] = {}

        # files: file_hash -> {"metadata": dict, "peers": set(peer_ids)}
        self.files: Dict[str, Dict[str, Any]] = {}

        self._prune_thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Starts the tracker server socket and background pruning thread."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(100)
        self.running = True

        # Background thread for accepting client connections
        self._listener_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._listener_thread.start()

        # Background thread to drop silent peers
        self._prune_thread = threading.Thread(target=self._prune_loop, daemon=True)
        self._prune_thread.start()

        print(f"[Tracker] Online and listening on {self.host}:{self.port}")

    def _accept_loop(self) -> None:
        """Accepts incoming TCP client connections continuously."""
        while self.running:
            try:
                client_sock, client_addr = self.server_socket.accept()
                client_sock.settimeout(SOCKET_TIMEOUT)
                handler_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_sock, client_addr),
                    daemon=True,
                )
                handler_thread.start()
            except OSError:
                break

    def run_forever(self) -> None:
        """Runs the tracker server continuously until interrupted."""
        if not self.running:
            self.start()
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

    def stop(self) -> None:
        """Stops the tracker server and closes socket."""
        self.running = False
        if self.server_socket:
            try:
                self.server_socket.close()
            except OSError:
                pass
            self.server_socket = None
        print("[Tracker] Stopped.")

    def _prune_loop(self) -> None:
        """Periodically checks and removes stale peers that missed heartbeats."""
        while self.running:
            time.sleep(5)
            now = time.time()
            stale_peers = []

            with self.lock:
                for peer_id, info in self.peers.items():
                    if now - info["last_seen"] > TRACKER_STALE_TIMEOUT:
                        stale_peers.append(peer_id)

                for peer_id in stale_peers:
                    self._remove_peer_internal(peer_id)

            if stale_peers:
                print(f"[Tracker] Pruned stale peers: {stale_peers}")

    def _remove_peer_internal(self, peer_id: str) -> None:
        """Internal helper to clean peer from registry (called within lock)."""
        if peer_id in self.peers:
            del self.peers[peer_id]

        for file_hash, file_info in list(self.files.items()):
            file_info["peers"].discard(peer_id)
            if not file_info["peers"]:
                # Optionally delete file if no peers share it
                del self.files[file_hash]

    def _handle_client(self, sock: socket.socket, addr: tuple) -> None:
        """Handles incoming messages from a peer client."""
        try:
            while self.running:
                try:
                    header, _ = recv_message(sock)
                except (ConnectionError, TimeoutError):
                    break

                msg_type = header.get("type")
                response_header: Dict[str, Any] = {}

                with self.lock:
                    if msg_type == MSG_REGISTER_PEER:
                        peer_id = header["peer_id"]
                        peer_port = header["peer_port"]
                        # Use provided host or fallback to peer's incoming IP
                        peer_host = header.get("peer_host") or addr[0]

                        self.peers[peer_id] = {
                            "host": peer_host,
                            "port": peer_port,
                            "last_seen": time.time(),
                            "files": self.peers.get(peer_id, {}).get("files", set()),
                        }
                        response_header = {
                            "type": MSG_REGISTER_PEER_ACK,
                            "status": "OK",
                            "message": f"Peer '{peer_id}' registered successfully",
                        }

                    elif msg_type == MSG_UNREGISTER_PEER:
                        peer_id = header.get("peer_id")
                        if peer_id:
                            self._remove_peer_internal(peer_id)
                        response_header = {
                            "type": MSG_UNREGISTER_PEER_ACK,
                            "status": "OK",
                        }

                    elif msg_type == MSG_HEARTBEAT:
                        peer_id = header.get("peer_id")
                        if peer_id and peer_id in self.peers:
                            self.peers[peer_id]["last_seen"] = time.time()
                            response_header = {"type": MSG_HEARTBEAT_ACK, "status": "OK"}
                        else:
                            response_header = {
                                "type": MSG_ERROR,
                                "message": "Peer not registered",
                            }

                    elif msg_type == MSG_REGISTER_FILE:
                        peer_id = header.get("peer_id")
                        metadata = header.get("metadata", {})
                        file_hash = metadata.get("file_hash")

                        if not peer_id or peer_id not in self.peers:
                            response_header = {
                                "type": MSG_ERROR,
                                "message": f"Peer '{peer_id}' not registered",
                            }
                        elif not file_hash:
                            response_header = {
                                "type": MSG_ERROR,
                                "message": "Missing file_hash in metadata",
                            }
                        else:
                            self.peers[peer_id]["files"].add(file_hash)
                            if file_hash not in self.files:
                                self.files[file_hash] = {
                                    "metadata": metadata,
                                    "peers": set(),
                                }
                            self.files[file_hash]["peers"].add(peer_id)
                            response_header = {
                                "type": MSG_REGISTER_FILE_ACK,
                                "status": "OK",
                                "file_name": metadata.get("file_name"),
                                "file_hash": file_hash,
                            }

                    elif msg_type == MSG_LIST_FILES:
                        file_list = []
                        for f_hash, f_info in self.files.items():
                            file_list.append(
                                {
                                    "file_name": f_info["metadata"]["file_name"],
                                    "file_size": f_info["metadata"]["file_size"],
                                    "file_hash": f_hash,
                                    "total_chunks": f_info["metadata"]["total_chunks"],
                                    "chunk_size": f_info["metadata"]["chunk_size"],
                                    "seeders_count": len(f_info["peers"]),
                                }
                            )
                        response_header = {
                            "type": MSG_LIST_FILES_ACK,
                            "files": file_list,
                        }

                    elif msg_type == MSG_GET_FILE_PEERS:
                        file_hash = header.get("file_hash")
                        if file_hash in self.files:
                            seeders = []
                            for p_id in self.files[file_hash]["peers"]:
                                if p_id in self.peers:
                                    p_info = self.peers[p_id]
                                    seeders.append(
                                        {
                                            "peer_id": p_id,
                                            "host": p_info["host"],
                                            "port": p_info["port"],
                                        }
                                    )
                            response_header = {
                                "type": MSG_GET_FILE_PEERS_ACK,
                                "status": "OK",
                                "metadata": self.files[file_hash]["metadata"],
                                "peers": seeders,
                            }
                        else:
                            response_header = {
                                "type": MSG_GET_FILE_PEERS_ACK,
                                "status": "NOT_FOUND",
                                "peers": [],
                                "message": "File not registered on network",
                            }

                    else:
                        response_header = {
                            "type": MSG_ERROR,
                            "message": f"Unknown message type: {msg_type}",
                        }

                send_message(sock, response_header)

        except Exception as e:
            pass
        finally:
            sock.close()


class TrackerClient:
    """
    Client helper class for Peers to interact with the Tracker.
    """

    def __init__(self, tracker_host: str = DEFAULT_TRACKER_HOST, tracker_port: int = DEFAULT_TRACKER_PORT):
        self.tracker_host = tracker_host
        self.tracker_port = tracker_port

    def _query(self, request_header: Dict[str, Any]) -> Dict[str, Any]:
        """Opens a short-lived TCP connection, sends request, receives response."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(SOCKET_TIMEOUT)
        try:
            sock.connect((self.tracker_host, self.tracker_port))
            send_message(sock, request_header)
            response, _ = recv_message(sock)
            return response
        except (socket.error, ConnectionError, TimeoutError) as e:
            raise ConnectionError(f"Tracker unreachable at {self.tracker_host}:{self.tracker_port}: {e}")
        finally:
            sock.close()

    def register_peer(self, peer_id: str, peer_port: int, peer_host: str = "127.0.0.1") -> Dict[str, Any]:
        return self._query(
            {
                "type": MSG_REGISTER_PEER,
                "peer_id": peer_id,
                "peer_host": peer_host,
                "peer_port": peer_port,
            }
        )

    def unregister_peer(self, peer_id: str) -> Dict[str, Any]:
        try:
            return self._query({"type": MSG_UNREGISTER_PEER, "peer_id": peer_id})
        except ConnectionError:
            return {"status": "UNREACHABLE"}

    def heartbeat(self, peer_id: str) -> Dict[str, Any]:
        return self._query({"type": MSG_HEARTBEAT, "peer_id": peer_id})

    def register_file(self, peer_id: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        return self._query(
            {
                "type": MSG_REGISTER_FILE,
                "peer_id": peer_id,
                "metadata": metadata,
            }
        )

    def list_files(self) -> List[Dict[str, Any]]:
        res = self._query({"type": MSG_LIST_FILES})
        return res.get("files", [])

    def get_file_peers(self, file_hash: str) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
        res = self._query({"type": MSG_GET_FILE_PEERS, "file_hash": file_hash})
        if res.get("status") == "OK":
            return res.get("metadata"), res.get("peers", [])
        return None, []
