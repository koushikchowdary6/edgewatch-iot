"""Only finite, Unicode-scalar JSON can authorize replay outbox deletion."""

import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from replay import _strict_json, flush


class Response(io.BytesIO):
    status = 200


class ReplayScalarIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "outbox.jsonl"
        self.event = '{"device":"sensor-1","seq":7}\n'
        self.path.write_text(self.event)

    def flush_ack(self, ack):
        with patch("replay.urllib.request.urlopen", return_value=Response(ack)):
            return flush(self.path, "http://localhost", "key")

    def test_overflowing_json_exponents_in_ack_do_not_dequeue(self):
        for numeric in (b"1e999", b"-1e999", b"1e309"):
            with self.subTest(numeric=numeric):
                ack = (b'{"accepted":true,"duplicate":false,'
                       b'"device":"sensor-1","seq":7,"meta":' + numeric + b'}')
                self.assertEqual(self.flush_ack(ack), 0)
                self.assertEqual(self.path.read_text(), self.event)

    def test_nonfinite_number_in_queued_record_does_not_dequeue(self):
        record = '{"device":"sensor-1","seq":7,"diagnostic":1e999}\n'
        self.path.write_text(record)
        ack = b'{"accepted":true,"duplicate":false,"device":"sensor-1","seq":7}'
        self.assertEqual(self.flush_ack(ack), 0)
        self.assertEqual(self.path.read_text(), record)

    def test_unpaired_unicode_surrogates_in_ack_do_not_dequeue(self):
        for bad in (b'"\\ud800"', b'"\\udfff"', b'{"\\ud800":1}'):
            with self.subTest(bad=bad):
                ack = (b'{"accepted":true,"duplicate":false,'
                       b'"device":"sensor-1","seq":7,"meta":' + bad + b'}')
                self.assertEqual(self.flush_ack(ack), 0)
                self.assertEqual(self.path.read_text(), self.event)

    def test_unpaired_surrogate_in_queued_record_does_not_dequeue(self):
        record = '{"device":"sensor-1","seq":7,"meta":"\\ud800"}\n'
        self.path.write_text(record)
        ack = b'{"accepted":true,"duplicate":false,"device":"sensor-1","seq":7}'
        self.assertEqual(self.flush_ack(ack), 0)
        self.assertEqual(self.path.read_text(), record)

    def test_valid_unicode_pair_and_finite_exponent_still_dequeue(self):
        ack = (b'{"accepted":true,"duplicate":false,"device":"sensor-1",'
               b'"seq":7,"note":"\\ud83d\\ude80","diagnostic":1e3}')
        self.assertEqual(self.flush_ack(ack), 1)
        self.assertEqual(self.path.read_text(), "")

    def test_scalar_validation_applies_to_nested_keys(self):
        with self.assertRaisesRegex(ValueError, "surrogate"):
            _strict_json(b'{"meta":{"\\ud800":"value"}}')


if __name__ == "__main__":
    unittest.main()
