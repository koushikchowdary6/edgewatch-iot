"""Replay must not discard telemetry on ambiguous or nonstandard JSON ACKs."""
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from replay import flush


class Response(io.BytesIO):
    status = 200


class StrictReplayAcknowledgmentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'outbox.jsonl'
        self.event = '{"device":"sensor-1","seq":7}\n'
        self.path.write_text(self.event)

    def _flush_with_ack(self, ack):
        with patch('replay.urllib.request.urlopen', return_value=Response(ack)):
            return flush(self.path, 'http://localhost', 'key')

    def test_duplicate_acknowledgment_keys_do_not_dequeue(self):
        acknowledgments = [
            b'{"accepted":false,"accepted":true,"duplicate":false,"device":"sensor-1","seq":7}',
            b'{"accepted":true,"duplicate":false,"device":"sensor-1","seq":7,"seq":7}',
            b'{"accepted":true,"duplicate":false,"device":"sensor-1","seq":7,"meta":{"a":1,"a":2}}',
        ]
        for ack in acknowledgments:
            with self.subTest(ack=ack):
                self.assertEqual(self._flush_with_ack(ack), 0)
                self.assertEqual(self.path.read_text(), self.event)

    def test_nonstandard_numeric_constants_do_not_dequeue(self):
        for constant in (b'NaN', b'Infinity', b'-Infinity'):
            with self.subTest(constant=constant):
                ack = (b'{"accepted":true,"duplicate":false,"device":"sensor-1",'
                       b'"seq":7,"diagnostic":' + constant + b'}')
                self.assertEqual(self._flush_with_ack(ack), 0)
                self.assertEqual(self.path.read_text(), self.event)

    def test_ambiguous_queued_event_is_not_dequeued_on_fake_ack(self):
        malformed = '{"device":"sensor-1","seq":6,"seq":7}\n'
        self.path.write_text(malformed)
        ack = b'{"accepted":true,"duplicate":false,"device":"sensor-1","seq":7}'
        self.assertEqual(self._flush_with_ack(ack), 0)
        self.assertEqual(self.path.read_text(), malformed)

    def test_deeply_nested_acknowledgment_is_rejected_without_dequeue(self):
        ack = (b'{"accepted":true,"duplicate":false,"device":"sensor-1",'
               b'"seq":7,"diagnostic":' + b'[' * 65 + b'0' + b']' * 65 + b'}')
        self.assertEqual(self._flush_with_ack(ack), 0)
        self.assertEqual(self.path.read_text(), self.event)

    def test_deeply_nested_queued_record_is_not_dequeued(self):
        malformed = ('{"device":"sensor-1","seq":7,"meta":'
                     + '[' * 65 + '0' + ']' * 65 + '}\\n')
        self.path.write_text(malformed)
        ack = b'{"accepted":true,"duplicate":false,"device":"sensor-1","seq":7}'
        self.assertEqual(self._flush_with_ack(ack), 0)
        self.assertEqual(self.path.read_text(), malformed)

    def test_brackets_inside_quoted_ack_metadata_are_not_nesting(self):
        ack = (b'{"accepted":true,"duplicate":false,"device":"sensor-1",'
               b'"seq":7,"diagnostic":"[' + b'[' * 80 + b'\\\\"quoted\\\\"' + b']' * 80 + b']"}')
        self.assertEqual(self._flush_with_ack(ack), 1)
        self.assertEqual(self.path.read_text(), '')

    def test_unambiguous_ack_still_dequeues(self):
        ack = b'{"accepted":true,"duplicate":false,"device":"sensor-1","seq":7}'
        self.assertEqual(self._flush_with_ack(ack), 1)
        self.assertEqual(self.path.read_text(), '')


if __name__ == '__main__':
    unittest.main()
