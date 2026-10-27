"""
Configuration constants and settings for the P2P File Distribution System.
Suitable for local network or localhost college project demonstrations.
"""

# Default Tracker (Discovery Server) configuration
DEFAULT_TRACKER_HOST = "127.0.0.1"
DEFAULT_TRACKER_PORT = 8000

# Default Peer Server configuration
DEFAULT_PEER_HOST = "127.0.0.1"
DEFAULT_PEER_PORT = 5001

# Chunk size for file splitting (in bytes)
# 64 KB per chunk allows demonstrating chunked multi-peer transfers
# even on modest file sizes (e.g. 512 KB = 8 chunks across peers).
DEFAULT_CHUNK_SIZE = 64 * 1024  # 65536 bytes (64 KB)

# Network socket parameters
SOCKET_TIMEOUT = 8.0          # Seconds to wait before timing out a socket operation
SOCKET_BUFFER_SIZE = 64 * 1024 # Read buffer size for TCP stream
MAX_LISTEN_BACKLOG = 50       # Backlog for socket listen()

# Concurrency & Retries
MAX_CONCURRENT_DOWNLOADS = 8  # Number of concurrent chunk download threads
MAX_CHUNK_RETRIES = 3         # Retries per chunk before marking transfer failed
PEER_HEARTBEAT_INTERVAL = 15  # Seconds between peer heartbeat signals to tracker
TRACKER_STALE_TIMEOUT = 45    # Seconds of silence after which tracker drops a peer

# Message protocol magic number / version
PROTOCOL_VERSION = "P2P_V1.0"
