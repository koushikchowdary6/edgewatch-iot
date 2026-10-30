"""Out-of-order delivery must not roll back a device's high-water sequence."""
from datetime import datetime, timedelta, timezone
import unittest

from app import connect, ingest, overview

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
BASE = dict(device='device-a', seq=10, ts=NOW.isoformat(), temperature=25, voltage=3.8)


class OutOfOrderSequenceTests(unittest.TestCase):
    def setUp(self):
        self.db = connect(':memory:')
        self.addCleanup(self.db.close)

    def test_late_older_sequence_keeps_high_water_but_refreshes_receipt(self):
        ingest(self.db, BASE, received_at=NOW)
        later = NOW + timedelta(seconds=5)
        ingest(self.db, dict(BASE, seq=2), received_at=later)
        state = overview(self.db, now=later)
        device = state['devices'][0]
        self.assertEqual(device['last_seq'], 10)
        self.assertEqual(device['last_seen'], later.isoformat())
        self.assertEqual(device['status'], 'healthy')
        self.assertEqual(state['stored_readings'], 2)

    def test_duplicate_older_sequence_does_not_move_high_water(self):
        ingest(self.db, BASE, received_at=NOW)
        older = dict(BASE, seq=2)
        ingest(self.db, older, received_at=NOW + timedelta(seconds=1))
        self.assertTrue(ingest(self.db, older, received_at=NOW + timedelta(seconds=2))['duplicate'])
        state = overview(self.db, now=NOW + timedelta(seconds=2))
        self.assertEqual(state['devices'][0]['last_seq'], 10)
        self.assertEqual(state['counters']['accepted'], 2)
        self.assertEqual(state['counters']['duplicates'], 1)

    def test_device_high_water_marks_are_independent(self):
        ingest(self.db, BASE, received_at=NOW)
        ingest(self.db, dict(BASE, device='device-b', seq=1), received_at=NOW)
        ingest(self.db, dict(BASE, seq=3), received_at=NOW)
        devices = {row['device']: row['last_seq'] for row in overview(self.db, now=NOW)['devices']}
        self.assertEqual(devices, {'device-a': 10, 'device-b': 1})


if __name__ == '__main__':
    unittest.main()
