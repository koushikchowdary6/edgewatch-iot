"""JSONL framing must not confuse Unicode characters with record delimiters."""
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from replay import flush


class Ack(io.BytesIO):
    status = 201


class JsonlFramingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'outbox.jsonl'

    def test_unicode_line_separators_inside_json_string_are_one_event(self):
        # These are valid JSON string characters, but str.splitlines() treats
        # them as record boundaries. Only a literal LF separates JSONL rows.
        for separator in ('\u0085', '\u2028', '\u2029'):
            with self.subTest(separator=hex(ord(separator))):
                payload = json.dumps({'note': 'before' + separator + 'after'}, ensure_ascii=False)
                self.path.write_text(payload + '\n', encoding='utf-8')
                requests = []

                def deliver(request, timeout):
                    requests.append(request.data)
                    return Ack(b'{"accepted":true,"duplicate":false}')

                with patch('replay.urllib.request.urlopen', side_effect=deliver):
                    self.assertEqual(flush(self.path, 'http://localhost', 'key'), 1)
                self.assertEqual(requests, [payload.encode('utf-8')])
                self.assertEqual(self.path.read_bytes(), b'')

    def test_unicode_separator_is_preserved_when_network_fails(self):
        payload = json.dumps({'note': 'line\u2028within'}, ensure_ascii=False) + '\n'
        self.path.write_text(payload, encoding='utf-8')
        with patch('replay.urllib.request.urlopen', side_effect=OSError('offline')) as send:
            self.assertEqual(flush(self.path, 'http://localhost', 'key'), 0)
            self.assertEqual(send.call_count, 1)
        self.assertEqual(self.path.read_text(encoding='utf-8'), payload)

    def test_empty_outbox_does_not_make_network_request(self):
        self.path.write_bytes(b'')
        with patch('replay.urllib.request.urlopen') as send:
            self.assertEqual(flush(self.path, 'http://localhost', 'key'), 0)
            send.assert_not_called()
