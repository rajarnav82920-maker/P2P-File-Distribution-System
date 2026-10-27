"""
Unit Tests for Tracker Server and Peer Discovery Logic.

Tests peer registration, file indexing, peer discovery, and heartbeat
using bidirectional mock sockets.
"""

import unittest
from src.tracker import TrackerServer
from src.protocol import (
    MSG_REGISTER_PEER,
    MSG_REGISTER_PEER_ACK,
    MSG_REGISTER_FILE,
    MSG_REGISTER_FILE_ACK,
    MSG_LIST_FILES,
    MSG_LIST_FILES_ACK,
    MSG_GET_FILE_PEERS,
    MSG_GET_FILE_PEERS_ACK,
    MSG_HEARTBEAT,
    MSG_HEARTBEAT_ACK,
    MSG_UNREGISTER_PEER,
    send_message,
    recv_message,
)
from tests.test_protocol import create_mock_socket_pair


class TestTrackerLogic(unittest.TestCase):

    def setUp(self):
        self.tracker = TrackerServer(host="127.0.0.1", port=0)
        self.tracker.running = True

    def _exchange(self, header: dict) -> dict:
        """Sends a request to the tracker using bidirectional mock socket pair and receives response header."""
        client_sock, server_sock = create_mock_socket_pair()

        # Send request
        send_message(client_sock, header)
        # Half-close write side so tracker handler finishes loop after processing
        client_sock.shutdown_write()

        # Process on server
        self.tracker._handle_client(server_sock, ("127.0.0.1", 5001))

        # Read tracker response from client's receive buffer
        resp_header, _ = recv_message(client_sock)
        client_sock.close()
        server_sock.close()
        return resp_header

    def test_peer_registration_and_heartbeat(self):
        # 1. Register peer
        reg_header = {
            "type": MSG_REGISTER_PEER,
            "peer_id": "UnitPeer_1",
            "peer_host": "127.0.0.1",
            "peer_port": 5001,
        }
        res = self._exchange(reg_header)
        self.assertEqual(res.get("type"), MSG_REGISTER_PEER_ACK)
        self.assertEqual(res.get("status"), "OK")
        self.assertIn("UnitPeer_1", self.tracker.peers)

        # 2. Heartbeat
        hb_header = {"type": MSG_HEARTBEAT, "peer_id": "UnitPeer_1"}
        res = self._exchange(hb_header)
        self.assertEqual(res.get("type"), MSG_HEARTBEAT_ACK)

    def test_file_registration_and_discovery(self):
        # Register peer first
        self._exchange({
            "type": MSG_REGISTER_PEER,
            "peer_id": "Seeder_A",
            "peer_host": "127.0.0.1",
            "peer_port": 6001,
        })

        metadata = {
            "file_name": "college_project.pdf",
            "file_size": 1048576,
            "chunk_size": 65536,
            "total_chunks": 16,
            "file_hash": "a1b2c3d4e5f60000",
            "chunk_hashes": ["hash_" + str(i) for i in range(16)],
        }

        # Register file
        res = self._exchange({
            "type": MSG_REGISTER_FILE,
            "peer_id": "Seeder_A",
            "metadata": metadata,
        })
        self.assertEqual(res.get("type"), MSG_REGISTER_FILE_ACK)
        self.assertIn("a1b2c3d4e5f60000", self.tracker.files)

        # List files
        list_res = self._exchange({"type": MSG_LIST_FILES})
        self.assertEqual(list_res.get("type"), MSG_LIST_FILES_ACK)
        files = list_res.get("files", [])
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0]["file_name"], "college_project.pdf")
        self.assertEqual(files[0]["seeders_count"], 1)

        # Get file peers
        peers_res = self._exchange({
            "type": MSG_GET_FILE_PEERS,
            "file_hash": "a1b2c3d4e5f60000",
        })
        self.assertEqual(peers_res.get("type"), MSG_GET_FILE_PEERS_ACK)
        self.assertEqual(peers_res.get("status"), "OK")
        seeders = peers_res.get("peers", [])
        self.assertEqual(len(seeders), 1)
        self.assertEqual(seeders[0]["peer_id"], "Seeder_A")
        self.assertEqual(seeders[0]["port"], 6001)

    def test_unregister_peer(self):
        self._exchange({
            "type": MSG_REGISTER_PEER,
            "peer_id": "LeavingPeer",
            "peer_host": "127.0.0.1",
            "peer_port": 6002,
        })
        self.assertIn("LeavingPeer", self.tracker.peers)

        res = self._exchange({"type": MSG_UNREGISTER_PEER, "peer_id": "LeavingPeer"})
        self.assertEqual(res.get("type"), "UNREGISTER_PEER_ACK")
        self.assertNotIn("LeavingPeer", self.tracker.peers)


if __name__ == "__main__":
    unittest.main()
