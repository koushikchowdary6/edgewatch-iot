"""Reproduce connectivity loss and safe replay, then inspect the dashboard."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import secrets
import threading
from http.server import ThreadingHTTPServer
from app import Config, connect, handler, overview
from replay import flush


def run(output, port=8080, serve=True):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    db_path, outbox = output/'telemetry.sqlite3', output/'outbox.jsonl'
    key = os.environ.get('EDGEWATCH_KEY') or secrets.token_hex(24)
    config = Config(stale_seconds=5, offline_seconds=15)
    connect(db_path).close()
    def start(port):
        server = ThreadingHTTPServer(('127.0.0.1',port), handler(db_path,key,config))
        worker = threading.Thread(target=server.serve_forever)
        worker.start()
        return server, worker
    server, worker = start(port)
    actual_port = server.server_port
    url = f'http://127.0.0.1:{actual_port}'
    def record(seq, temperature):
        return dict(device='sim-esp32', seq=seq, ts=datetime.now(timezone.utc).isoformat(),temperature=temperature,voltage=3.8)
    outbox.write_text(json.dumps(record(0,24))+'\n')
    assert flush(outbox,url,key)==1
    server.shutdown(); worker.join(); server.server_close()
    buffered = ''.join(json.dumps(record(i,t))+'\n' for i,t in [(1,25),(2,72),(3,26)])
    outbox.write_text(buffered)
    assert flush(outbox,url,key)==0 and outbox.read_text()==buffered
    print('OUTAGE: 3 readings retained in the device outbox')
    server, worker = start(actual_port)
    try:
        assert flush(outbox,url,key)==3
        outbox.write_text(buffered)  # simulate crash before local acknowledgment persisted
        assert flush(outbox,url,key)==3
        with connect(db_path) as db:
            state = overview(db,config)
        assert state['stored_readings']==4 and state['stored_alerts']==1
        assert state['counters']['duplicates']==3
        summary = {'stored_readings':4,'stored_alerts':1,'duplicate_retries':3,'pending_outbox_lines':0}
        (output/'demo-result.json').write_text(json.dumps(summary,indent=2)+'\n')
        print('RECOVERED: '+json.dumps(summary))
        if serve:
            print(f'Dashboard: {url}\nSession API key: {key}\nCtrl-C to stop. Health becomes stale after 5s and offline after 15s.')
            worker.join()
    finally:
        server.shutdown(); worker.join(); server.server_close()
    return summary

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=Path('demo-data'))
    parser.add_argument('--port',type=int,default=8080)
    parser.add_argument('--no-serve',action='store_true')
    args=parser.parse_args()
    try:
        run(args.output,args.port,not args.no_serve)
    except KeyboardInterrupt:
        pass
    except (OSError,ValueError) as exc:
        parser.exit(1,f'Demo could not start: {exc}\nChoose a new output directory and an available port.\n')
