"""Replay a synthetic device's durable local outbox; retain failed records."""
import argparse
import json
import os
import stat
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# Bound memory consumed by an untrusted or accidentally oversized local queue.
# Oversized queues remain untouched until the operator rotates/repairs them.
MAX_OUTBOX_BYTES = 8_000_000


def _read_outbox(path):
    """Read only a bounded regular outbox; never follow a symlink or block on a FIFO."""
    flags = os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0)
    fd = os.open(path, flags)
    with os.fdopen(fd, 'rb') as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError('Outbox must be a regular file')
        data = stream.read(MAX_OUTBOX_BYTES + 1)
    if len(data) > MAX_OUTBOX_BYTES:
        raise ValueError('Outbox exceeds byte budget; rotate or repair it before replay')
    return data.decode('utf-8')


def _quarantine(path, line, status):
    """Persist a permanently rejected record before removing it from the outbox.

    This file contains the original telemetry and must be handled as sensitive
    operational data. O_APPEND and fsync make a failed write stop replay without
    dropping the outbox. A crash can still duplicate a quarantine entry.
    """
    record = json.dumps({'http_status': status, 'record': line}) + '\n'
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, 'O_NONBLOCK', 0)
    if hasattr(os, 'O_NOFOLLOW'):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags, 0o600)
    with os.fdopen(fd, 'a', encoding='utf-8') as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError('Quarantine must be a regular file')
        stream.write(record)
        stream.flush()
        os.fsync(stream.fileno())


def flush(path, url, key, quarantine=None):
    path = Path(path)
    quarantine = Path(quarantine) if quarantine is not None else None
    if quarantine is not None and quarantine.resolve() == path.resolve():
        raise ValueError('Quarantine must differ from the outbox')
    # JSONL records are separated by LF, not every Unicode line-break codepoint.
    # str.splitlines() would split valid JSON strings containing U+0085,
    # U+2028 or U+2029, corrupting a record before delivery/quarantine.
    raw = _read_outbox(path)
    records = raw.split('\n') if raw else []
    if records and records[-1] == '':
        records.pop()  # A final LF terminates the last record; it is not a blank event.
    processed = 0
    sent = 0
    for line in records:
        request = urllib.request.Request(url + '/api/samples', line.encode(),
            {'Content-Type': 'application/json', 'X-API-Key': key})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                if response.status not in (200, 201):
                    break
                # A proxy/login page can return 200 without storing telemetry.
                # Only an explicit receiver acknowledgment permits dequeueing.
                try:
                    body = response.read(4097)
                    if len(body) > 4096:
                        break
                    ack = json.loads(body)
                except (ValueError, UnicodeError):
                    break
                if (not isinstance(ack, dict)
                        or type(ack.get('accepted')) is not bool
                        or type(ack.get('duplicate')) is not bool
                        or ack['accepted'] == ack['duplicate']):
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
    if not processed:
        return sent
    # Unique same-directory staging avoids a predictable .tmp collision.
    fd, temporary = tempfile.mkstemp(prefix='.outbox-', dir=path.parent)
    temp = Path(temporary)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            stream.write('\n'.join(records[processed:]) + ('\n' if records[processed:] else ''))
            stream.flush()
            os.fsync(stream.fileno())
        temp.replace(path)
    finally:
        temp.unlink(missing_ok=True)
    return sent


def replay_until(path, url, key, attempts=3, delay=0.25, quarantine=None):
    """Bounded retry of retained records. One writer per outbox; no background daemon."""
    if type(attempts) is not int or not 1 <= attempts <= 20:
        raise ValueError('Attempts must be 1..20')
    if type(delay) not in (int, float) or not 0 <= delay <= 30:
        raise ValueError('Retry delay must be 0..30 seconds')
    total = 0
    for attempt in range(attempts):
        total += flush(path, url, key, quarantine)
        if not _read_outbox(Path(path)).strip():
            break
        if attempt + 1 < attempts:
            time.sleep(delay * min(2 ** attempt, 4))
    return total


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--url', default='http://127.0.0.1:8080')
    p.add_argument('--outbox', type=Path, default=Path('outbox.jsonl'))
    p.add_argument('--quarantine', type=Path, help='Opt in to quarantining HTTP 400/413 records')
    p.add_argument('--attempts', type=int, default=3)
    p.add_argument('--retry-delay', type=float, default=0.25)
    p.add_argument('--seed', action='store_true')
    a = p.parse_args()
    if a.seed:
        if a.outbox.exists():
            p.error('Outbox exists; drain or move it before seeding')
        now = datetime.now(timezone.utc).isoformat()
        a.outbox.write_text(''.join(json.dumps(dict(device='sim-esp32', seq=i, ts=now,
            temperature=72 if i == 8 else 24 + i / 10, voltage=3.0 if i == 9 else 3.8)) + '\n' for i in range(10)))
    print({'sent': replay_until(a.outbox, a.url, os.environ.get('EDGEWATCH_KEY', ''), a.attempts, a.retry_delay, a.quarantine),
           'pending_lines': len(a.outbox.read_text().splitlines())})
