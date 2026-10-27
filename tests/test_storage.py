"""
Unit Tests for Storage, Chunking, and Hash Integrity.
"""

import os
import shutil
import unittest
from src.storage import (
    compute_sha256,
    compute_file_sha256,
    FileManager,
    FileAssembler,
)


class TestStorage(unittest.TestCase):

    def setUp(self):
        self.test_dir = os.path.abspath("test_storage_scratch")
        self.shared_dir = os.path.join(self.test_dir, "shared")
        self.dl_dir = os.path.join(self.test_dir, "dl")
        os.makedirs(self.shared_dir, exist_ok=True)
        os.makedirs(self.dl_dir, exist_ok=True)

        self.chunk_size = 1024  # 1 KB chunk size for fast unit testing
        self.file_manager = FileManager(self.shared_dir, chunk_size=self.chunk_size)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_chunking_and_metadata(self):
        filename = "test_file.txt"
        file_path = os.path.join(self.shared_dir, filename)
        # Create 2500 bytes (will form 3 chunks: 1024, 1024, 452)
        content = b"A" * 1024 + b"B" * 1024 + b"C" * 452
        with open(file_path, "wb") as f:
            f.write(content)

        meta = self.file_manager.generate_metadata(filename)
        self.assertEqual(meta["file_name"], filename)
        self.assertEqual(meta["file_size"], 2500)
        self.assertEqual(meta["chunk_size"], 1024)
        self.assertEqual(meta["total_chunks"], 3)
        self.assertEqual(len(meta["chunk_hashes"]), 3)
        self.assertEqual(meta["file_hash"], compute_sha256(content))

        # Check individual chunk hashes
        self.assertEqual(meta["chunk_hashes"][0], compute_sha256(b"A" * 1024))
        self.assertEqual(meta["chunk_hashes"][1], compute_sha256(b"B" * 1024))
        self.assertEqual(meta["chunk_hashes"][2], compute_sha256(b"C" * 452))

    def test_read_chunk(self):
        filename = "test_read.txt"
        file_path = os.path.join(self.shared_dir, filename)
        with open(file_path, "wb") as f:
            f.write(b"Hello Chunk 0" + b"\x00" * (1024 - 13) + b"Hello Chunk 1")

        c0 = self.file_manager.read_chunk(filename, 0)
        self.assertEqual(len(c0), 1024)
        self.assertTrue(c0.startswith(b"Hello Chunk 0"))

        c1 = self.file_manager.read_chunk(filename, 1)
        self.assertEqual(c1, b"Hello Chunk 1")

    def test_assembler_and_verification(self):
        filename = "assemble_me.bin"
        file_path = os.path.join(self.shared_dir, filename)
        content = b"DATA_PIECE_1" * 100 + b"DATA_PIECE_2" * 100
        with open(file_path, "wb") as f:
            f.write(content)

        meta = self.file_manager.generate_metadata(filename)
        assembler = FileAssembler(self.dl_dir, meta)

        self.assertFalse(assembler.is_complete())
        self.assertEqual(len(assembler.get_missing_chunks()), meta["total_chunks"])

        # Feed chunks one by one
        for i in range(meta["total_chunks"]):
            chunk_data = self.file_manager.read_chunk(filename, i)
            saved = assembler.save_chunk(i, chunk_data)
            self.assertTrue(saved)

        self.assertTrue(assembler.is_complete())
        final_file = assembler.assemble()
        self.assertTrue(os.path.exists(final_file))
        self.assertEqual(compute_file_sha256(final_file), meta["file_hash"])

    def test_corrupted_chunk_rejection(self):
        filename = "corrupt_test.bin"
        file_path = os.path.join(self.shared_dir, filename)
        with open(file_path, "wb") as f:
            f.write(b"GOOD_DATA_ORIGINAL")

        meta = self.file_manager.generate_metadata(filename)
        assembler = FileAssembler(self.dl_dir, meta)

        # Attempt to save tampered bytes
        corrupted_data = b"TAMPERED_BYTES!!!!!"
        saved = assembler.save_chunk(0, corrupted_data)
        self.assertFalse(saved)
        self.assertFalse(assembler.is_complete())


if __name__ == "__main__":
    unittest.main()
