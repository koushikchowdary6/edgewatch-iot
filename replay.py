"""Replay a synthetic device's durable local outbox; retain failed records."""
import argparse
import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


def _quarantine(path, line, status):
    """Persist a permanently rejected record before removing it from the outbox.

    This file contains the original telemetry and must be handled as sensitive
    operational data. O_APPEND and fsync make a failed write stop replay without
    dropping the outbox. A crash can still duplicate a quarantine entry.
    """
    record = json.dumps({'http_status': status, 'record': line}) + '\n'
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
    if hasattr(os, 'O_NOFOLLOW'):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags, 0o600)
    with os.fdopen(fd, 'a', encoding='utf-8') as stream:
        stream.write(record)
        stream.flush()
        os.fsync(stream.fileno())


def flush(path, url, key, quarantine=None):
    path = Path(path)
    quarantine = Path(quarantine) if quarantine is not None else None
    if quarantine is not None and quarantine.resolve() == path.resolve():
        raise ValueError('Quarantine must differ from the outbox')
    records = path.read_text().splitlines()
    processed = 0
    sent = 0
    for line in records:
        request = urllib.request.Request(url + '/api/samples', line.encode(),
            {'Content-Type': 'application/json', 'X-API-Key': key})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                if response.status not in (200, 201):
                    break
        except urllib.error.HTTPError as error:
            # Invalid payloads cannot succeed on retry without intervention.
            # Auth, concurrency, rate-limit and server failures stay queued.
            if quarantine is None or error.code not in (400, 413):
                break
            _quarantine(quarantine, line, error.code)
            processed += 1
            continue
        except OSError:
            break
        sent += 1
        processed += 1
    temp = path.with_suffix('.tmp')
    temp.write_text('\n'.join(records[processed:]) + ('\n' if records[processed:] else ''))
    temp.replace(path)
    return sent


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--url', default='http://127.0.0.1:8080')
    p.add_argument('--outbox', type=Path, default=Path('outbox.jsonl'))
    p.add_argument('--quarantine', type=Path, help='Opt in to quarantining HTTP 400/413 records')
    p.add_argument('--seed', action='store_true')
    a = p.parse_args()
    if a.seed:
        if a.outbox.exists():
            p.error('Outbox exists; drain or move it before seeding')
        now = datetime.now(timezone.utc).isoformat()
        a.outbox.write_text(''.join(json.dumps(dict(device='sim-esp32', seq=i, ts=now,
            temperature=72 if i == 8 else 24 + i / 10, voltage=3.0 if i == 9 else 3.8)) + '\n' for i in range(10)))
    print({'sent': flush(a.outbox, a.url, os.environ.get('EDGEWATCH_KEY', ''), a.quarantine)})
