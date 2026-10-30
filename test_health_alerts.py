from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest
from app import Config, connect, ingest, overview, strict_json

SAMPLE = dict(device='sim-1',seq=1,ts='2026-01-01T00:00:00+00:00',temperature=70,voltage=3.8)
NOW = datetime(2026,1,1,tzinfo=timezone.utc)

class HealthAlertsTests(unittest.TestCase):
    def setUp(self):
        self.db=connect(':memory:')
        self.addCleanup(self.db.close)

    def test_health_transitions_use_server_receipt_time(self):
        ingest(self.db,dict(SAMPLE,ts='2099-01-01T00:00:00+00:00'),received_at=NOW)
        for age,status in [(0,'healthy'),(30,'healthy'),(31,'stale'),(120,'stale'),(121,'offline')]:
            with self.subTest(age=age):
                self.assertEqual(overview(self.db,now=NOW+timedelta(seconds=age))['devices'][0]['status'],status)

    def test_naive_and_invalid_ingestion_receipt_clocks_fail_before_writing(self):
        for value in [datetime(2026, 1, 1), '2026-01-01T00:00:00Z', 0]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                ingest(self.db, SAMPLE, received_at=value)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM samples').fetchone()[0], 0)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM devices').fetchone()[0], 0)

    def test_naive_health_snapshot_clock_is_rejected(self):
        ingest(self.db, SAMPLE, received_at=NOW)
        for value in [datetime(2026, 1, 1), '2026-01-01', 0]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                overview(self.db, now=value)

    def test_offset_aware_receipt_is_normalized_to_utc(self):
        offset = timezone(timedelta(hours=5, minutes=30))
        ingest(self.db, SAMPLE, received_at=datetime(2026, 1, 1, 5, 30, tzinfo=offset))
        state = overview(self.db, now=NOW)
        self.assertEqual(state['devices'][0]['last_seen'], NOW.isoformat())
        self.assertEqual(state['devices'][0]['status'], 'healthy')

    def test_replay_after_threshold_change_does_not_conflict_or_add_alert(self):
        ingest(self.db,SAMPLE,received_at=NOW)
        self.assertTrue(ingest(self.db,SAMPLE,Config(temperature_high=80))['duplicate'])
        state=overview(self.db,now=NOW)
        self.assertEqual(state['stored_alerts'],1)
        self.assertEqual(state['alerts'][0]['threshold'],60)
        self.assertEqual(state['devices'][0]['last_seen'],NOW.isoformat())

    def test_normal_recovery_keeps_historical_alert(self):
        ingest(self.db,SAMPLE)
        ingest(self.db,dict(SAMPLE,seq=2,temperature=24))
        state=overview(self.db)
        self.assertEqual(state['stored_alerts'],1)
        self.assertEqual(state['stored_readings'],2)

    def test_configurable_rules_capture_both_limits(self):
        ingest(self.db,dict(SAMPLE,voltage=3.4),Config(temperature_high=65,voltage_low=3.5))
        self.assertEqual({x['threshold'] for x in overview(self.db)['alerts']},{65,3.5})

    def test_invalid_configuration(self):
        for kwargs in [{'temperature_high':float('nan')},{'stale_seconds':0},{'offline_seconds':20},{'voltage_low':True}]:
            with self.subTest(kwargs=kwargs),self.assertRaises(ValueError):Config(**kwargs)

    def test_duplicate_json_keys_rejected(self):
        with self.assertRaises(ValueError): strict_json('{"seq":1,"seq":2}')

    def test_concurrent_retries_persist_one_sample_and_alert(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'db';connect(path).close()
            def submit(_):
                db=connect(path)
                try:return ingest(db,SAMPLE)
                finally:db.close()
            with ThreadPoolExecutor(max_workers=4) as pool:
                results=list(pool.map(submit,range(8)))
            self.assertEqual(sum(x['accepted'] for x in results),1)
            db=connect(path)
            try:
                state=overview(db)
                self.assertEqual(state['stored_alerts'],1)
                self.assertEqual(state['counters']['duplicates'],7)
            finally:db.close()

    def test_restart_preserves_health_alerts_and_counters(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'db';db=connect(path);ingest(db,SAMPLE,received_at=NOW);db.close()
            db=connect(path)
            try:
                state=overview(db,now=NOW+timedelta(seconds=200))
                self.assertEqual(state['devices'][0]['status'],'offline')
                self.assertEqual(state['counters']['accepted'],1)
                self.assertEqual(state['stored_alerts'],1)
            finally:db.close()

    def test_legacy_readings_have_unknown_health(self):
        self.db.execute('INSERT INTO samples VALUES (?,?,?,?,?,?)',('legacy',1,NOW.isoformat(),25,3.8,''))
        self.db.commit()
        self.assertEqual(overview(self.db)['devices'][0]['status'],'unknown')
