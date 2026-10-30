"""Local telemetry reliability lab. Python 3.10+, standard library only."""
import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import hmac
import json
import logging
import math
import os
from pathlib import Path
import sqlite3
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOG = logging.getLogger('edgewatch')

@dataclass(frozen=True)
class Config:
    temperature_high: float = 60
    voltage_low: float = 3.2
    stale_seconds: float = 30
    offline_seconds: float = 120

    def __post_init__(self):
        values = (self.temperature_high, self.voltage_low, self.stale_seconds, self.offline_seconds)
        if any(type(x) not in (int, float) or not math.isfinite(x) for x in values):
            raise ValueError('Configuration must contain finite numbers')
        if not (-80 <= self.temperature_high <= 150 and 0 <= self.voltage_low <= 60
                and 0 < self.stale_seconds < self.offline_seconds):
            raise ValueError('Invalid thresholds or health timeouts')


def utcnow():
    return datetime.now(timezone.utc)


def connect(path):
    db = sqlite3.connect(path, timeout=5)
    db.execute('PRAGMA foreign_keys=ON')
    db.executescript('''
    CREATE TABLE IF NOT EXISTS samples(device TEXT, seq INTEGER, ts TEXT,
      temperature REAL, voltage REAL, alert TEXT, PRIMARY KEY(device,seq));
    CREATE TABLE IF NOT EXISTS devices(device TEXT PRIMARY KEY, last_seen TEXT NOT NULL,
      last_seq INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS alerts(device TEXT, seq INTEGER, kind TEXT,
      value REAL NOT NULL, threshold REAL NOT NULL, received_at TEXT NOT NULL,
      PRIMARY KEY(device,seq,kind), FOREIGN KEY(device,seq) REFERENCES samples(device,seq));
    CREATE TABLE IF NOT EXISTS counters(name TEXT PRIMARY KEY, value INTEGER NOT NULL);
    ''')
    return db


def increment(db, name):
    db.execute('INSERT INTO counters VALUES (?,1) ON CONFLICT(name) DO UPDATE SET value=value+1', (name,))


def ingest(db, sample, config=None, received_at=None):
    config = config or Config()
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
    if not isinstance(sample['ts'], str):
        raise ValueError('Timestamp must be a string')
    ts = datetime.fromisoformat(sample['ts'])
    if ts.tzinfo is None:
        raise ValueError('Timestamp needs timezone')
    ts = ts.astimezone(timezone.utc).isoformat()
    received = (received_at or utcnow()).astimezone(timezone.utc).isoformat()
    flags = []
    if sample['temperature'] > config.temperature_high:
        flags.append(('HIGH_TEMPERATURE', sample['temperature'], config.temperature_high))
    if sample['voltage'] < config.voltage_low:
        flags.append(('LOW_VOLTAGE', sample['voltage'], config.voltage_low))
    alert = ', '.join(x[0] for x in flags)
    row = (device, seq, ts, sample['temperature'], sample['voltage'], alert)
    with db:
        # Serialize the read/check/insert so concurrent retries cannot duplicate alerts.
        db.execute('BEGIN IMMEDIATE')
        existing = db.execute('SELECT * FROM samples WHERE device=? AND seq=?', (device, seq)).fetchone()
        if existing:
            # Compare the event, not today's derived alert configuration.
            if existing[:5] != row[:5]:
                raise ValueError('Sequence conflict: replay payload changed')
            increment(db, 'duplicates')
            return {'accepted': False, 'duplicate': True, 'alert': existing[-1]}
        db.execute('INSERT INTO samples VALUES (?,?,?,?,?,?)', row)
        db.execute('INSERT INTO devices VALUES (?,?,?) ON CONFLICT(device) DO UPDATE SET last_seen=excluded.last_seen,last_seq=MAX(devices.last_seq, excluded.last_seq)', (device, received, seq))
        db.executemany('INSERT INTO alerts VALUES (?,?,?,?,?,?)', [(device, seq, kind, value, threshold, received) for kind, value, threshold in flags])
        increment(db, 'accepted')
    return {'accepted': True, 'duplicate': False, 'alert': alert}


def snapshot(db):
    rows = db.execute('SELECT * FROM samples ORDER BY ts DESC, device, seq DESC LIMIT 200').fetchall()
    return [dict(zip(['device', 'seq', 'ts', 'temperature', 'voltage', 'alert'], row)) for row in rows]


def overview(db, config=None, now=None):
    config, now = config or Config(), now or utcnow()
    devices = []
    for device, seen, seq in db.execute('SELECT s.device,d.last_seen,d.last_seq FROM (SELECT DISTINCT device FROM samples) s LEFT JOIN devices d ON s.device=d.device ORDER BY s.device'):
        age = max(0, (now - datetime.fromisoformat(seen)).total_seconds()) if seen else None
        status = ('unknown' if age is None else 'offline' if age > config.offline_seconds
                  else 'stale' if age > config.stale_seconds else 'healthy')
        devices.append({'device': device, 'last_seen': seen, 'last_seq': seq, 'age_seconds': age, 'status': status})
    alerts = [dict(zip(['device','seq','kind','value','threshold','received_at'], row)) for row in db.execute('SELECT * FROM alerts ORDER BY received_at DESC, device, seq DESC, kind LIMIT 100')]
    counters = {'accepted': 0, 'duplicates': 0, 'rejected': 0}
    counters.update(dict(db.execute('SELECT * FROM counters')))
    return {'devices': devices, 'alerts': alerts, 'samples': snapshot(db), 'counters': counters,
            'stored_readings': db.execute('SELECT COUNT(*) FROM samples').fetchone()[0],
            'stored_alerts': db.execute('SELECT COUNT(*) FROM alerts').fetchone()[0],
            'config': config.__dict__, 'server_time': now.isoformat()}


def strict_json(data):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('Duplicate JSON key')
            result[key] = value
        return result
    return json.loads(data, object_pairs_hook=pairs)


def handler(db_path, key, config=None):
    config = config or Config()
    if len(key) < 16 or not key.isascii():
        raise ValueError('API key must be at least 16 ASCII characters')
    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            super().setup()
            self.connection.settimeout(5)
            self.request_id = uuid.uuid4().hex
            self.started = time.monotonic()

        def log_message(self, *_):
            pass

        def reply(self, code, body, content_type='application/json'):
            data = body.encode() if isinstance(body, str) else json.dumps(body, allow_nan=False).encode()
            self.send_response(code)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(data)))
            self.send_header('Cache-Control', 'no-store')
            self.send_header('X-Request-ID', self.request_id)
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.send_header('Content-Security-Policy', "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; frame-ancestors 'none'")
            self.end_headers()
            self.wfile.write(data)
            LOG.info(json.dumps({'request_id': self.request_id, 'method': self.command,
                                'status': code, 'latency_ms': round((time.monotonic()-self.started)*1000, 2)}))

        def error(self, code, name, message):
            if self.command == 'POST':
                try:
                    with connect(db_path) as db:
                        increment(db, 'rejected')
                except sqlite3.Error:
                    pass
            self.reply(code, {'error': {'code': name, 'message': message}, 'request_id': self.request_id})

        def authorized(self):
            return hmac.compare_digest(self.headers.get('X-API-Key', '').encode('utf-8'), key.encode('ascii'))

        def do_GET(self):
            if self.path == '/':
                return self.reply(200, Path(__file__).with_name('dashboard.html').read_text(), 'text/html; charset=utf-8')
            if self.path == '/healthz':
                return self.reply(200, {'status': 'ok'})
            if self.path not in ('/api/samples', '/api/overview'):
                return self.error(404, 'not_found', 'Route does not exist')
            if not self.authorized():
                return self.error(401, 'unauthorized', 'Valid X-API-Key required')
            try:
                with connect(db_path) as db:
                    self.reply(200, snapshot(db) if self.path == '/api/samples' else overview(db, config))
            except sqlite3.Error:
                self.error(503, 'storage_unavailable', 'Storage unavailable; retry later')

        def do_POST(self):
            if self.path != '/api/samples':
                return self.error(404, 'not_found', 'Route does not exist')
            if not self.authorized():
                return self.error(401, 'unauthorized', 'Valid X-API-Key required')
            try:
                length = int(self.headers.get('Content-Length', '0'))
                if not 0 < length <= 4096:
                    return self.error(413, 'body_limit', 'Body must be 1..4096 bytes')
                if self.headers.get('Transfer-Encoding'):
                    return self.error(400, 'unsupported_encoding', 'Chunked requests not supported')
                sample = strict_json(self.rfile.read(length))
                with connect(db_path) as db:
                    result = ingest(db, sample, config)
                self.reply(201 if result['accepted'] else 200, result)
            except (ValueError, TypeError, KeyError, OverflowError, RecursionError):
                self.error(400, 'invalid_telemetry', 'Invalid telemetry or conflicting replay')
            except sqlite3.Error:
                self.error(503, 'storage_unavailable', 'Storage unavailable; retry unchanged event')
            except OSError:
                self.error(408, 'request_timeout', 'Request body incomplete or timed out')
    return Handler


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', default='edgewatch.sqlite3')
    parser.add_argument('--port', type=int, default=8080)
    parser.add_argument('--temperature-high', type=float, default=60)
    parser.add_argument('--voltage-low', type=float, default=3.2)
    parser.add_argument('--stale-seconds', type=float, default=30)
    parser.add_argument('--offline-seconds', type=float, default=120)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    try:
        config = Config(args.temperature_high, args.voltage_low, args.stale_seconds, args.offline_seconds)
        server = ThreadingHTTPServer(('127.0.0.1', args.port), handler(args.db, os.environ.get('EDGEWATCH_KEY', ''), config))
    except ValueError as exc:
        parser.error(str(exc))
    connect(args.db).close()
    print(f'EdgeWatch http://127.0.0.1:{server.server_port} — synthetic telemetry lab')
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
