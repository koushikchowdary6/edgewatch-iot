"""Only receiver-originated permanent errors can remove queued telemetry."""
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from replay import flush


class QuarantineEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.outbox = Path(self.temp.name) / 'outbox.jsonl'
        self.quarantine = Path(self.temp.name) / 'rejected.jsonl'
        self.event = '{"device":"sim","seq":1}\n'
        self.outbox.write_text(self.event)

    def error(self, status, body, content_type='application/json'):
        return HTTPError('http://localhost/api/samples', status, 'rejected',
                         {'Content-Type': content_type}, io.BytesIO(body))

    def run_error(self, error):
        with patch('replay.urllib.request.urlopen', side_effect=error):
            return flush(self.outbox, 'http://localhost', 'key', self.quarantine)

    def test_proxy_and_malformed_error_bodies_leave_outbox_unchanged(self):
        invalid = [
            self.error(400, b'<html>proxy denied</html>', 'text/html'),
            self.error(413, b'{"error":"too large"}'),
            self.error(400, b'{"error":{"code":"invalid_telemetry","message":"bad"}}'),
            self.error(400, b'{"error":{"code":"invalid_telemetry","message":"bad"},"request_id":"bad"}'),
            self.error(400, b'{"error":{"code":"invalid_telemetry","message":"bad"},"request_id":"a' + b'0'*31 + b'","request_id":"a"}'),
            self.error(400, b'{}' * 3000),
            self.error(400, b'{"error":{"code":"body_limit","message":"bad"},"request_id":"' + b'a'*32 + b'"}'),
            self.error(413, b'{"error":{"code":"invalid_telemetry","message":"bad"},"request_id":"' + b'a'*32 + b'"}'),
        ]
        for error in invalid:
            with self.subTest(status=error.code):
                self.assertEqual(self.run_error(error), 0)
                self.assertEqual(self.outbox.read_text(), self.event)
                self.assertFalse(self.quarantine.exists())

    def test_confirmed_receiver_rejections_are_durably_quarantined(self):
        for status, code in [(400, 'invalid_telemetry'), (400, 'invalid_framing'), (413, 'body_limit')]:
            with self.subTest(status=status, code=code):
                self.outbox.write_text(self.event)
                self.quarantine.unlink(missing_ok=True)
                body = json.dumps({'error': {'code': code, 'message': 'invalid'},
                                   'request_id': 'f' * 32}).encode()
                self.assertEqual(self.run_error(self.error(status, body)), 0)
                self.assertEqual(self.outbox.read_text(), '')
                record = json.loads(self.quarantine.read_text())
                self.assertEqual(record['record'], self.event.rstrip('\n'))
                self.assertEqual(record['http_status'], status)


if __name__ == '__main__':
    unittest.main()
