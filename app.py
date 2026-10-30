"""Offline IoT telemetry receiver and dashboard. Python 3.10+, no dependencies."""
import argparse
import hmac
import json
import math
import os
import sqlite3
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def connect(path):
    db = sqlite3.connect(path)
    db.execute('CREATE TABLE IF NOT EXISTS samples(device TEXT, seq INTEGER, ts TEXT, temperature REAL, voltage REAL, alert TEXT, PRIMARY KEY(device,seq))')
    return db


def ingest(db, sample):
    if not isinstance(sample, dict) or set(sample) != {'device', 'seq', 'ts', 'temperature', 'voltage'}:
        raise ValueError('Expected device, seq, ts, temperature, voltage')
    device, seq = sample['device'], sample['seq']
    if not isinstance(device, str) or not device.isascii() or not device.replace('-', '').isalnum() or len(device) > 64:
        raise ValueError('Invalid device')
    if type(seq) is not int or not 0 <= seq < 2**63:
        raise ValueError('Invalid sequence')
    for key, low, high in [('temperature', -80, 150), ('voltage', 0, 60)]:
        value = sample[key]
        if type(value) not in (int, float) or not math.isfinite(value) or not low <= value <= high:
            raise ValueError('Invalid ' + key)
    ts = datetime.fromisoformat(sample['ts'])
    if ts.tzinfo is None:
        raise ValueError('Timestamp needs timezone')
    ts = ts.astimezone(timezone.utc).isoformat()
    alert = ', '.join(x for flag, x in [(sample['temperature'] > 60, 'HIGH_TEMPERATURE'), (sample['voltage'] < 3.2, 'LOW_VOLTAGE')] if flag)
    row = (device, seq, ts, sample['temperature'], sample['voltage'], alert)
    existing = db.execute('SELECT * FROM samples WHERE device=? AND seq=?', (device, seq)).fetchone()
    if existing:
        if existing != row:
            raise ValueError('Sequence conflict: replay payload changed')
        return {'accepted': False, 'duplicate': True, 'alert': existing[-1]}
    with db:
        db.execute('INSERT INTO samples VALUES (?,?,?,?,?,?)', row)
    return {'accepted': True, 'duplicate': False, 'alert': alert}


def snapshot(db):
    rows = db.execute('SELECT * FROM samples ORDER BY ts DESC, device, seq DESC LIMIT 200').fetchall()
    return [dict(zip(['device', 'seq', 'ts', 'temperature', 'voltage', 'alert'], row)) for row in rows]


def handler(db_path, key):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def reply(self, code, body, content_type='application/json'):
            data = body.encode() if isinstance(body, str) else json.dumps(body).encode()
            self.send_response(code)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(data)))
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.path == '/':
                return self.reply(200, Path(__file__).with_name('dashboard.html').read_text(), 'text/html; charset=utf-8')
            if self.path == '/healthz':
                return self.reply(200, {'status': 'ok'})
            if self.path != '/api/samples':
                return self.reply(404, {'error': 'Not found'})
            if not hmac.compare_digest(self.headers.get('X-API-Key', ''), key):
                return self.reply(401, {'error': 'Unauthorized'})
            with connect(db_path) as db:
                self.reply(200, snapshot(db))

        def do_POST(self):
            if self.path != '/api/samples':
                return self.reply(404, {'error': 'Not found'})
            if not hmac.compare_digest(self.headers.get('X-API-Key', ''), key):
                return self.reply(401, {'error': 'Unauthorized'})
            try:
                length = int(self.headers.get('Content-Length', '0'))
                if not 0 < length <= 4096:
                    return self.reply(413, {'error': 'Body must be 1..4096 bytes'})
                sample = json.loads(self.rfile.read(length))
                with connect(db_path) as db:
                    result = ingest(db, sample)
                self.reply(201 if result['accepted'] else 200, result)
            except (ValueError, TypeError, KeyError, OverflowError):
                self.reply(400, {'error': 'Invalid telemetry or conflicting replay'})
            except sqlite3.IntegrityError:
                self.reply(409, {'error': 'Concurrent replay; retry unchanged payload'})
    return Handler


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default='edgewatch.sqlite3')
    parser.add_argument('--port', type=int, default=8080)
    args = parser.parse_args()
    key = os.environ.get('EDGEWATCH_KEY', '')
    if len(key) < 16 or not key.isascii():
        parser.error('Set EDGEWATCH_KEY to at least 16 ASCII characters')
    connect(args.db).close()
    print(f'EdgeWatch: http://127.0.0.1:{args.port} (synthetic replay demo)')
    ThreadingHTTPServer(('127.0.0.1', args.port), handler(args.db, key)).serve_forever()
