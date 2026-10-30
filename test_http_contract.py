import json
from pathlib import Path
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from app import connect, handler

class HttpContractTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.path=Path(self.temp.name)/'db';connect(self.path).close()
        self.server=ThreadingHTTPServer(('127.0.0.1',0),handler(self.path,'x'*16))
        self.thread=threading.Thread(target=self.server.serve_forever);self.thread.start()
        self.addCleanup(self.stop)
        self.url=f'http://127.0.0.1:{self.server.server_port}'
    def stop(self):
        self.server.shutdown();self.thread.join();self.server.server_close()
    def test_validation_error_has_request_id_and_persisted_rejection(self):
        request=urllib.request.Request(self.url+'/api/samples',b'{"seq":1,"seq":2}',{'X-API-Key':'x'*16})
        with self.assertRaises(urllib.error.HTTPError) as caught:urllib.request.urlopen(request)
        error=caught.exception
        self.assertEqual(error.code,400)
        data=json.load(error)
        self.assertEqual(data['request_id'],error.headers['X-Request-ID'])
        self.assertEqual(data['error']['code'],'invalid_telemetry')
        request=urllib.request.Request(self.url+'/api/overview',headers={'X-API-Key':'x'*16})
        with urllib.request.urlopen(request) as response:
            self.assertEqual(json.load(response)['counters']['rejected'],1)
    def test_overview_requires_authentication(self):
        with self.assertRaises(urllib.error.HTTPError) as caught:urllib.request.urlopen(self.url+'/api/overview')
        self.assertEqual(caught.exception.code,401)
