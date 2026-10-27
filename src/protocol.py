"""
TCP Network Protocol & Framing Layer.

Provides length-prefixed message framing over raw TCP sockets.
This prevents TCP stream fragmentation / concatenation issues.

Packet Structure:
  [4-byte Header Length (Big-Endian)]
  [JSON Header Bytes (UTF-8)]
  [4-byte Payload Length (Big-Endian)]
  [Raw Binary Payload Bytes (e.g., chunk data)]
"""

import json
import struct
import socket
from typing import Tuple, Dict, Any, Optional

# Message Type Constants
MSG_HANDSHAKE = "HANDSHAKE"
MSG_HANDSHAKE_ACK = "HANDSHAKE_ACK"

# Tracker Messages
MSG_REGISTER_PEER = "REGISTER_PEER"
MSG_REGISTER_PEER_ACK = "REGISTER_PEER_ACK"
MSG_UNREGISTER_PEER = "UNREGISTER_PEER"
MSG_UNREGISTER_PEER_ACK = "UNREGISTER_PEER_ACK"
MSG_REGISTER_FILE = "REGISTER_FILE"
MSG_REGISTER_FILE_ACK = "REGISTER_FILE_ACK"
MSG_LIST_FILES = "LIST_FILES"
MSG_LIST_FILES_ACK = "LIST_FILES_ACK"
MSG_GET_FILE_PEERS = "GET_FILE_PEERS"
MSG_GET_FILE_PEERS_ACK = "GET_FILE_PEERS_ACK"
MSG_HEARTBEAT = "HEARTBEAT"
MSG_HEARTBEAT_ACK = "HEARTBEAT_ACK"

# Peer-to-Peer Messages
MSG_GET_METADATA = "GET_METADATA"
MSG_METADATA_RESPONSE = "METADATA_RESPONSE"
MSG_GET_CHUNK = "GET_CHUNK"
MSG_CHUNK_RESPONSE = "CHUNK_RESPONSE"
MSG_PING = "PING"
MSG_PONG = "PONG"
MSG_ERROR = "ERROR"


def recv_exact(sock: socket.socket, num_bytes: int) -> bytes:
    """
    Reads exactly `num_bytes` from the socket, looping until all bytes are received.
    Raises ConnectionError if connection closes prematurely or ConnectionResetError occurs.
    """
    buffer = bytearray()
    while len(buffer) < num_bytes:
        remaining = num_bytes - len(buffer)
        try:
            chunk = sock.recv(remaining)
        except (socket.timeout, TimeoutError):
            raise TimeoutError("Socket operation timed out while waiting for data")
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError) as e:
            raise ConnectionError(f"Connection reset/aborted by remote peer: {e}")
        except OSError as e:
            raise ConnectionError(f"Socket OS error: {e}")

        if not chunk:
            raise ConnectionError("Connection closed unexpectedly by peer (EOF reached)")
        buffer.extend(chunk)
    return bytes(buffer)


def send_message(sock: socket.socket, header: Dict[str, Any], payload: bytes = b"") -> None:
    """
    Sends a framed message over TCP:
    [4 bytes: Header Length] + [UTF-8 JSON Header] + [4 bytes: Payload Length] + [Payload bytes]
    """
    try:
        header_json = json.dumps(header).encode("utf-8")
        header_len = len(header_json)
        payload_len = len(payload)

        # Pack lengths as 4-byte unsigned big-endian integers
        packet = struct.pack("!II", header_len, payload_len) + header_json + payload
        sock.sendall(packet)
    except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError) as e:
        raise ConnectionError(f"Failed to send message: {e}")
    except OSError as e:
        raise ConnectionError(f"Socket send error: {e}")


def recv_message(sock: socket.socket) -> Tuple[Dict[str, Any], bytes]:
    """
    Receives and decodes a framed message from TCP:
    Returns (header_dict, payload_bytes).
    """
    # 1. Read 8 bytes: two 4-byte integers (header_len, payload_len)
    length_prefix = recv_exact(sock, 8)
    header_len, payload_len = struct.unpack("!II", length_prefix)

    # 2. Read JSON header
    header_bytes = recv_exact(sock, header_len)
    header = json.loads(header_bytes.decode("utf-8"))

    # 3. Read binary payload (if any)
    payload = recv_exact(sock, payload_len) if payload_len > 0 else b""

    return header, payload
