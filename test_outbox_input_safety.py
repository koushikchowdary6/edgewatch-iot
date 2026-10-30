"""Offline replay must not hang on special files or load unbounded queues."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from replay import flush, replay_until


class OutboxInputSafetyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / "outbox.jsonl"
        self.payload = '{"device":"sim"}\n'
        self.path.write_text(self.payload)

    @unittest.skipUnless(hasattr(os, "symlink") and hasattr(os, "O_NOFOLLOW"), "requires no-follow symlinks")
    def test_symlink_outbox_is_rejected_without_network_or_modification(self):
        alias = self.root / "alias.jsonl"
        alias.symlink_to(self.path)
        with patch("replay.urllib.request.urlopen") as request:
            with self.assertRaises(OSError):
                flush(alias, "http://localhost", "key")
            request.assert_not_called()
        self.assertEqual(self.path.read_text(), self.payload)

    @unittest.skipUnless(hasattr(os, "mkfifo") and hasattr(os, "O_NONBLOCK"), "requires nonblocking FIFO support")
    def test_fifo_outbox_is_rejected_without_hanging(self):
        fifo = self.root / "outbox.fifo"
        os.mkfifo(fifo)
        with patch("replay.urllib.request.urlopen") as request:
            with self.assertRaisesRegex(ValueError, "regular file"):
                flush(fifo, "http://localhost", "key")
            request.assert_not_called()

    def test_oversized_outbox_is_not_replayed_or_rewritten(self):
        with patch("replay.MAX_OUTBOX_BYTES", len(self.payload) - 1):
            with patch("replay.urllib.request.urlopen") as request:
                with self.assertRaisesRegex(ValueError, "byte budget"):
                    flush(self.path, "http://localhost", "key")
                request.assert_not_called()
        self.assertEqual(self.path.read_text(), self.payload)

    def test_retry_loop_uses_same_bounded_read(self):
        with patch("replay.MAX_OUTBOX_BYTES", len(self.payload) - 1):
            with self.assertRaisesRegex(ValueError, "byte budget"):
                replay_until(self.path, "http://localhost", "key", attempts=2)
        self.assertEqual(self.path.read_text(), self.payload)

    @unittest.skipUnless(hasattr(os, "symlink") and hasattr(os, "O_NOFOLLOW"), "requires no-follow symlinks")
    def test_quarantine_symlink_cannot_overwrite_other_file(self):
        target = self.root / "do-not-overwrite.txt"
        target.write_text("sentinel")
        quarantine = self.root / "quarantine.jsonl"
        quarantine.symlink_to(target)
        error = HTTPError("http://localhost/api/samples", 400, "bad telemetry", {}, None)
        with patch("replay.urllib.request.urlopen", side_effect=error):
            with self.assertRaises(OSError):
                flush(self.path, "http://localhost", "key", quarantine=quarantine)
        self.assertEqual(target.read_text(), "sentinel")
        self.assertEqual(self.path.read_text(), self.payload)
