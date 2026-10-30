"""Device health must not regress when accepted events have older receipt clocks."""
from datetime import datetime, timedelta, timezone
import unittest

from app import connect, ingest, overview

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
SAMPLE = dict(device='device-a', seq=1, ts=NOW.isoformat(), temperature=25, voltage=3.8)


class MonotonicReceiptClockTests(unittest.TestCase):
    def setUp(self):
        self.db = connect(':memory:')
        self.addCleanup(self.db.close)

    def test_older_receipt_cannot_move_last_seen_backward(self):
        newest = NOW + timedelta(seconds=60)
        ingest(self.db, SAMPLE, received_at=newest)
        ingest(self.db, dict(SAMPLE, seq=2), received_at=NOW)
        state = overview(self.db, now=newest + timedelta(seconds=20))
        self.assertEqual(state['devices'][0]['last_seen'], newest.isoformat())
        self.assertEqual(state['devices'][0]['last_seq'], 2)
        self.assertEqual(state['devices'][0]['status'], 'healthy')
        self.assertEqual(state['stored_readings'], 2)

    def test_newer_receipt_advances_clock_even_for_older_sequence(self):
        ingest(self.db, dict(SAMPLE, seq=10), received_at=NOW)
        later = NOW + timedelta(seconds=5)
        ingest(self.db, dict(SAMPLE, seq=2), received_at=later)
        state = overview(self.db, now=later)
        self.assertEqual(state['devices'][0]['last_seen'], later.isoformat())
        self.assertEqual(state['devices'][0]['last_seq'], 10)

    def test_duplicate_replay_does_not_refresh_receipt(self):
        ingest(self.db, SAMPLE, received_at=NOW)
        result = ingest(self.db, SAMPLE, received_at=NOW + timedelta(days=1))
        self.assertTrue(result['duplicate'])
        self.assertEqual(overview(self.db, now=NOW)['devices'][0]['last_seen'], NOW.isoformat())


if __name__ == '__main__':
    unittest.main()
