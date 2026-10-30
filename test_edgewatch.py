import copy
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
import json
from pathlib import Path
from http.server import ThreadingHTTPServer
from app import connect, ingest, handler
from replay import flush

SAMPLE = dict(device='sim-1', seq=1, ts='2026-01-01T00:00:00+00:00', temperature=25, voltage=3.8)

class Tests(unittest.TestCase):
    def setUp(self):
        self.db = connect(':memory:')
    def tearDown(self):
        self.db.close()
    def test_replay_is_idempotent(self):
        self.assertTrue(ingest(self.db, SAMPLE)['accepted'])
        self.assertTrue(ingest(self.db, SAMPLE)['duplicate'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM samples').fetchone()[0], 1)
    def test_conflict(self):
        ingest(self.db, SAMPLE)
        with self.assertRaises(ValueError): ingest(self.db, dict(SAMPLE, voltage=4))
    def test_alerts(self):
        self.assertEqual(ingest(self.db, dict(SAMPLE, temperature=70, voltage=3))['alert'], 'HIGH_TEMPERATURE, LOW_VOLTAGE')
    def test_bad_measurements(self):
        for field, value in [('voltage', float('nan')), ('temperature', True), ('seq', -1), ('ts', '2026-01-01')]:
            with self.subTest(field=field), self.assertRaises(ValueError): ingest(self.db, dict(SAMPLE, **{field: value}))
    def test_persistence(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'db'
            db=connect(path); ingest(db, SAMPLE); db.close()
            db=connect(path); self.assertTrue(ingest(db, SAMPLE)['duplicate']); db.close()
    def test_http_and_outbox(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'db'; connect(path).close()
            server=ThreadingHTTPServer(('127.0.0.1',0), handler(path,'x'*16))
            thread=threading.Thread(target=server.serve_forever); thread.start()
            url=f'http://127.0.0.1:{server.server_port}'
            try:
                outbox=Path(d)/'outbox'; outbox.write_text(json.dumps(SAMPLE)+'\n')
                self.assertEqual(flush(outbox,url,'wrong'),0)
                self.assertTrue(outbox.read_text())
                self.assertEqual(flush(outbox,url,'x'*16),1)
                self.assertEqual(outbox.read_text(),'')
                request=urllib.request.Request(url+'/api/samples',headers={'X-API-Key':'x'*16})
                with urllib.request.urlopen(request) as r: self.assertEqual(len(json.load(r)),1)
            finally:
                server.shutdown(); thread.join(); server.server_close()


    def test_quarantine_bad_record_then_deliver_valid_record(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            db_path = root / 'db'
            connect(db_path).close()
            server = ThreadingHTTPServer(('127.0.0.1', 0), handler(db_path, 'x' * 16))
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            try:
                outbox = root / 'outbox'
                rejected = dict(SAMPLE, device='invalid device')
                outbox.write_text(json.dumps(rejected) + '\n' + json.dumps(SAMPLE) + '\n')
                quarantine = root / 'rejected.jsonl'
                url = f'http://127.0.0.1:{server.server_port}'
                self.assertEqual(flush(outbox, url, 'x' * 16, quarantine), 1)
                self.assertEqual(outbox.read_text(), '')
                entry = json.loads(quarantine.read_text())
                self.assertEqual(entry['http_status'], 400)
                self.assertEqual(json.loads(entry['record']), rejected)
                with connect(db_path) as db:
                    self.assertEqual(db.execute('SELECT COUNT(*) FROM samples').fetchone()[0], 1)
            finally:
                server.shutdown()
                thread.join()
                server.server_close()

    def test_auth_failure_is_never_quarantined(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            db_path = root / 'db'
            connect(db_path).close()
            server = ThreadingHTTPServer(('127.0.0.1', 0), handler(db_path, 'x' * 16))
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            try:
                outbox = root / 'outbox'
                payload = json.dumps(SAMPLE) + '\n'
                outbox.write_text(payload)
                quarantine = root / 'rejected.jsonl'
                url = f'http://127.0.0.1:{server.server_port}'
                self.assertEqual(flush(outbox, url, 'wrong', quarantine), 0)
                self.assertEqual(outbox.read_text(), payload)
                self.assertFalse(quarantine.exists())
            finally:
                server.shutdown()
                thread.join()
                server.server_close()

    def test_failed_quarantine_write_retains_outbox(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            db_path = root / 'db'
            connect(db_path).close()
            server = ThreadingHTTPServer(('127.0.0.1', 0), handler(db_path, 'x' * 16))
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            try:
                outbox = root / 'outbox'
                payload = json.dumps(dict(SAMPLE, device='invalid device')) + '\n'
                outbox.write_text(payload)
                url = f'http://127.0.0.1:{server.server_port}'
                with self.assertRaises(OSError):
                    flush(outbox, url, 'x' * 16, root)
                self.assertEqual(outbox.read_text(), payload)
            finally:
                server.shutdown()
                thread.join()
                server.server_close()

    def test_outbox_cannot_be_its_own_quarantine(self):
        with tempfile.TemporaryDirectory() as d:
            outbox = Path(d) / 'outbox'
            outbox.write_text(json.dumps(SAMPLE) + '\n')
            with self.assertRaises(ValueError):
                flush(outbox, 'http://127.0.0.1:1', 'key', outbox)

if __name__ == '__main__': unittest.main()
