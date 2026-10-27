"""
Unit Tests for TCP Message Framing and Protocol Layer.

Tests packet serialization, length prefixing, and framing using simulated
bidirectional socket pairs.
"""

import unittest
from src.protocol import send_message, recv_message, recv_exact


class MockSocket:
    """
    Simulates a full-duplex TCP socket endpoint connected to a peer socket.
    Supports graceful write shutdown (half-close) and bidirectional communication.
    """

    def __init__(self, peer=None):
        self.peer = peer
        self.recv_buf = bytearray()
        self.send_closed = False
        self.recv_closed = False

    def sendall(self, data: bytes) -> None:
        if self.send_closed:
            raise ConnectionError("Socket write end is closed")
        if self.peer and not self.peer.recv_closed:
            self.peer.recv_buf.extend(data)

    def recv(self, max_bytes: int) -> bytes:
        if self.recv_closed:
            return b""
        if not self.recv_buf:
            if self.peer and self.peer.send_closed:
                return b""  # Remote end finished sending
            return b""
        chunk = bytes(self.recv_buf[:max_bytes])
        self.recv_buf = self.recv_buf[max_bytes:]
        return chunk

    def shutdown_write(self) -> None:
        """Signals EOF on outgoing direction while keeping receive buffer open."""
        self.send_closed = True

    def close(self) -> None:
        """Closes both send and receive directions."""
        self.send_closed = True
        self.recv_closed = True


def create_mock_socket_pair():
    """Creates a connected pair of bidirectional mock sockets."""
    s1 = MockSocket()
    s2 = MockSocket(peer=s1)
    s1.peer = s2
    return s1, s2


class TestProtocol(unittest.TestCase):

    def test_mock_socket_framing_roundtrip(self):
        """Tests sending and receiving JSON header + binary payload through framing."""
        client_sock, server_sock = create_mock_socket_pair()

        test_header = {"type": "CHUNK_REQ", "file_name": "video.mp4", "chunk_index": 7}
        test_payload = b"\xDE\xAD\xBE\xEF" * 512  # 2 KB binary payload

        # Client sends packet
        send_message(client_sock, test_header, test_payload)

        # Server reads packet
        received_header, received_payload = recv_message(server_sock)

        self.assertEqual(received_header, test_header)
        self.assertEqual(received_payload, test_payload)

        # Server replies
        send_message(server_sock, {"status": "OK"})
        reply_header, reply_payload = recv_message(client_sock)
        self.assertEqual(reply_header["status"], "OK")
        self.assertEqual(reply_payload, b"")

        client_sock.close()
        server_sock.close()

    def test_mock_socket_empty_payload(self):
        """Tests sending control messages with zero payload bytes."""
        client_sock, server_sock = create_mock_socket_pair()

        test_header = {"type": "HEARTBEAT", "peer_id": "Peer_5001"}
        send_message(client_sock, test_header)

        received_header, received_payload = recv_message(server_sock)
        self.assertEqual(received_header, test_header)
        self.assertEqual(received_payload, b"")

    def test_fragmented_stream_recv_exact(self):
        """Tests that recv_exact correctly reassembles bytes delivered in tiny fragments."""
        class FragmentedSocket:
            def __init__(self, data: bytes, fragment_size: int = 3):
                self.data = bytearray(data)
                self.fragment_size = fragment_size

            def recv(self, max_bytes: int) -> bytes:
                if not self.data:
                    return b""
                take = min(self.fragment_size, max_bytes, len(self.data))
                chunk = bytes(self.data[:take])
                self.data = self.data[take:]
                return chunk

        raw_bytes = b"Hello Antigravity P2P Fragmented TCP Test Data!"
        frag_sock = FragmentedSocket(raw_bytes, fragment_size=4)

        result = recv_exact(frag_sock, len(raw_bytes))
        self.assertEqual(result, raw_bytes)


if __name__ == "__main__":
    unittest.main()
