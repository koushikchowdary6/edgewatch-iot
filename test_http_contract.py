import http.client
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

    def raw_request(self, method, path, headers, body=b''):
        connection = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=5)
        try:
            connection.putrequest(method, path)
            for name, value in headers:
                connection.putheader(name, value)
            connection.endheaders(body)
            response = connection.getresponse()
            return response.status, json.loads(response.read())
        finally:
            connection.close()

    def test_duplicate_api_key_headers_are_not_accepted(self):
        status, body = self.raw_request('GET', '/api/overview', [
            ('X-API-Key', 'x' * 16),
            ('X-API-Key', 'x' * 16),
        ])
        self.assertEqual(status, 401)
        self.assertEqual(body['error']['code'], 'unauthorized')

    def test_duplicate_content_length_does_not_ingest(self):
        payload = json.dumps(dict(device='sim', seq=1,
            ts='2026-01-01T00:00:00+00:00', temperature=22, voltage=3.8)).encode()
        status, body = self.raw_request('POST', '/api/samples', [
            ('X-API-Key', 'x' * 16),
            ('Content-Length', str(len(payload))),
            ('Content-Length', str(len(payload))),
        ], payload)
        self.assertEqual(status, 400)
        self.assertEqual(body['error']['code'], 'invalid_framing')
        with connect(self.path) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM samples').fetchone()[0], 0)

    def test_transfer_encoding_with_content_length_is_rejected(self):
        payload = b'{}'
        status, body = self.raw_request('POST', '/api/samples', [
            ('X-API-Key', 'x' * 16),
            ('Content-Length', str(len(payload))),
            ('Transfer-Encoding', 'chunked'),
        ], payload)
        self.assertEqual(status, 400)
        self.assertEqual(body['error']['code'], 'invalid_framing')

    def test_nondecimal_content_length_is_rejected(self):
        status, body = self.raw_request('POST', '/api/samples', [
            ('X-API-Key', 'x' * 16),
            ('Content-Length', '+2'),
        ], b'{}')
        self.assertEqual(status, 400)
        self.assertEqual(body['error']['code'], 'invalid_framing')

    def test_deeply_nested_json_returns_structured_error(self):
        request=urllib.request.Request(self.url+'/api/samples',b'['*1500+b']'*1500,{'X-API-Key':'x'*16})
        with self.assertRaises(urllib.error.HTTPError) as caught:urllib.request.urlopen(request)
        self.assertEqual(caught.exception.code,400)
        self.assertEqual(json.load(caught.exception)['error']['code'],'invalid_telemetry')
