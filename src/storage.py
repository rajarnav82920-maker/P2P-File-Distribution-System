"""
File Storage, Chunking, and Hash Verification Engine.

Handles:
- Splitting files into fixed-size chunks (e.g. 64 KB)
- Calculating cryptographic SHA-256 hashes for whole files and individual chunks
- Verifying downloaded chunk integrity against tampering or transmission errors
- Assembling chunks in order to reconstruct the original file
"""

import os
import shutil
import hashlib
from typing import Dict, List, Any, Optional
from src.config import DEFAULT_CHUNK_SIZE


def compute_sha256(data: bytes) -> str:
    """Calculates SHA-256 hash of byte data."""
    return hashlib.sha256(data).hexdigest()


def compute_file_sha256(file_path: str, buffer_size: int = 65536) -> str:
    """Calculates SHA-256 hash of an entire file streaming from disk."""
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(buffer_size):
            hasher.update(chunk)
    return hasher.hexdigest()


class FileManager:
    """
    Manages local files, chunk extraction, and metadata generation.
    """

    def __init__(self, base_dir: str, chunk_size: int = DEFAULT_CHUNK_SIZE):
        self.base_dir = os.path.abspath(base_dir)
        self.chunk_size = chunk_size
        os.makedirs(self.base_dir, exist_ok=True)

    def generate_metadata(self, filename: str) -> Dict[str, Any]:
        """
        Scans a file in base_dir, splits it logically into chunks,
        computes SHA-256 for each chunk, and returns metadata dictionary.
        """
        file_path = os.path.join(self.base_dir, filename)
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found in storage: {filename}")

        file_size = os.path.getsize(file_path)
        chunk_hashes: List[str] = []
        hasher_whole = hashlib.sha256()

        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(self.chunk_size)
                if not chunk:
                    break
                hasher_whole.update(chunk)
                chunk_hashes.append(compute_sha256(chunk))

        total_chunks = len(chunk_hashes) if file_size > 0 else 0
        file_hash = hasher_whole.hexdigest() if file_size > 0 else compute_sha256(b"")

        return {
            "file_name": filename,
            "file_size": file_size,
            "chunk_size": self.chunk_size,
            "total_chunks": total_chunks,
            "file_hash": file_hash,
            "chunk_hashes": chunk_hashes,
        }

    def read_chunk(self, filename: str, chunk_index: int) -> bytes:
        """
        Reads a specific chunk from a file by index using seek.
        """
        file_path = os.path.join(self.base_dir, filename)
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {filename}")

        offset = chunk_index * self.chunk_size
        with open(file_path, "rb") as f:
            f.seek(offset)
            data = f.read(self.chunk_size)
            return data


class FileAssembler:
    """
    Manages downloading chunks into a staging directory and assembling the final file.
    """

    def __init__(self, target_dir: str, metadata: Dict[str, Any]):
        self.target_dir = os.path.abspath(target_dir)
        self.metadata = metadata
        self.file_name = metadata["file_name"]
        self.file_hash = metadata["file_hash"]
        self.file_size = metadata["file_size"]
        self.chunk_size = metadata["chunk_size"]
        self.total_chunks = metadata["total_chunks"]
        self.chunk_hashes = metadata["chunk_hashes"]

        os.makedirs(self.target_dir, exist_ok=True)
        self.staging_dir = os.path.join(self.target_dir, f".part_{self.file_hash[:12]}")
        os.makedirs(self.staging_dir, exist_ok=True)

        # Track which chunk indices have been downloaded and verified
        self.completed_chunks = set()
        self._scan_existing_parts()

    def _scan_existing_parts(self) -> None:
        """Check for any pre-existing verified chunk parts (resuming download)."""
        if self.total_chunks == 0:
            return
        for i in range(self.total_chunks):
            part_path = os.path.join(self.staging_dir, f"chunk_{i}.part")
            if os.path.isfile(part_path):
                with open(part_path, "rb") as f:
                    data = f.read()
                if compute_sha256(data) == self.chunk_hashes[i]:
                    self.completed_chunks.add(i)
                else:
                    try:
                        os.remove(part_path)
                    except OSError:
                        pass

    def save_chunk(self, chunk_index: int, data: bytes) -> bool:
        """
        Validates chunk data with expected SHA-256 hash.
        If valid, writes to disk staging area and marks completed.
        Returns True if chunk is valid, False otherwise.
        """
        if chunk_index < 0 or chunk_index >= self.total_chunks:
            return False

        expected_hash = self.chunk_hashes[chunk_index]
        actual_hash = compute_sha256(data)

        if actual_hash != expected_hash:
            return False

        part_path = os.path.join(self.staging_dir, f"chunk_{chunk_index}.part")
        with open(part_path, "wb") as f:
            f.write(data)

        self.completed_chunks.add(chunk_index)
        return True

    def is_complete(self) -> bool:
        """Checks if all chunks have been received and verified."""
        if self.total_chunks == 0:
            return True
        return len(self.completed_chunks) == self.total_chunks

    def get_missing_chunks(self) -> List[int]:
        """Returns list of chunk indices that still need to be downloaded."""
        return [i for i in range(self.total_chunks) if i not in self.completed_chunks]

    def assemble(self) -> str:
        """
        Assembles all verified chunk parts into the final destination file.
        Verifies whole-file SHA-256. If valid, cleans staging directory.
        Returns final file path.
        """
        if not self.is_complete():
            raise RuntimeError("Cannot assemble file: not all chunks are downloaded")

        final_path = os.path.join(self.target_dir, self.file_name)

        # Handle empty file edge case
        if self.total_chunks == 0 or self.file_size == 0:
            with open(final_path, "wb") as f:
                pass
            shutil.rmtree(self.staging_dir, ignore_errors=True)
            return final_path

        # Write sequential chunks into final output file
        with open(final_path, "wb") as outfile:
            for i in range(self.total_chunks):
                part_path = os.path.join(self.staging_dir, f"chunk_{i}.part")
                with open(part_path, "rb") as partfile:
                    outfile.write(partfile.read())

        # Final whole-file hash validation
        assembled_hash = compute_file_sha256(final_path)
        if assembled_hash != self.file_hash:
            try:
                os.remove(final_path)
            except OSError:
                pass
            raise ValueError(
                f"Assembled file hash mismatch! Expected {self.file_hash}, got {assembled_hash}"
            )

        # Clean staging parts
        shutil.rmtree(self.staging_dir, ignore_errors=True)
        return final_path
