"""Do not lose queued telemetry when acknowledgments or local writes fail."""
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from replay import flush

class Response(io.BytesIO):
    status = 200

class ReplayFailureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'outbox.jsonl'
        self.payload = '{"device":"sim","seq":7}\n'
        self.path.write_text(self.payload)

    def test_invalid_acknowledgments_keep_queue_unchanged(self):
        for ack in [b'<html>login</html>', b'{}', b'[]', b'{"accepted":true,"duplicate":true}', b'{"accepted":1,"duplicate":false}', b'{}' * 3000]:
            with self.subTest(ack=ack[:40]):
                before = self.path.stat().st_mtime_ns
                with patch('replay.urllib.request.urlopen', return_value=Response(ack)):
                    self.assertEqual(flush(self.path, 'http://localhost', 'key'), 0)
                self.assertEqual(self.path.read_text(), self.payload)
                self.assertEqual(self.path.stat().st_mtime_ns, before)

    def test_duplicate_acknowledgment_drains_record(self):
        with patch('replay.urllib.request.urlopen', return_value=Response(b'{"accepted":false,"duplicate":true,"device":"sim","seq":7}')):
            self.assertEqual(flush(self.path, 'http://localhost', 'key'), 1)
        self.assertEqual(self.path.read_text(), '')

    def test_unrelated_or_malformed_ack_cannot_dequeue_event(self):
        for ack in [
            b'{"accepted":true,"duplicate":false,"device":"other","seq":7}',
            b'{"accepted":false,"duplicate":true,"device":"sim","seq":8}',
            b'{"accepted":true,"duplicate":false,"device":"sim","seq":true}',
            b'{"accepted":true,"duplicate":false,"device":"sim"}',
        ]:
            with self.subTest(ack=ack):
                with patch('replay.urllib.request.urlopen', return_value=Response(ack)):
                    self.assertEqual(flush(self.path, 'http://localhost', 'key'), 0)
                self.assertEqual(self.path.read_text(), self.payload)

    def test_offline_failure_does_not_rewrite_outbox(self):
        before = self.path.stat().st_mtime_ns
        with patch('replay.urllib.request.urlopen', side_effect=OSError('offline')):
            self.assertEqual(flush(self.path, 'http://localhost', 'key'), 0)
        self.assertEqual(self.path.stat().st_mtime_ns, before)

    def test_replace_failure_preserves_queue_and_cleans_stage(self):
        with patch('replay.urllib.request.urlopen', return_value=Response(b'{"accepted":true,"duplicate":false,"device":"sim","seq":7}')), patch('replay.Path.replace', side_effect=OSError('disk failure')):
            with self.assertRaises(OSError): flush(self.path, 'http://localhost', 'key')
        self.assertEqual(self.path.read_text(), self.payload)
        self.assertEqual(list(self.path.parent.glob('.outbox-*')), [])

    def test_predictable_temp_file_is_not_touched(self):
        sentinel = self.path.with_suffix('.tmp')
        sentinel.write_text('do not overwrite')
        with patch('replay.urllib.request.urlopen', return_value=Response(b'{"accepted":true,"duplicate":false}')):
            flush(self.path, 'http://localhost', 'key')
        self.assertEqual(sentinel.read_text(), 'do not overwrite')
