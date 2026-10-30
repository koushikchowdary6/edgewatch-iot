from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from demo import run
from replay import replay_until

class DemoRetryTests(unittest.TestCase):
    def test_real_outage_reconnect_and_duplicate_demo(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(run(Path(d)/'demo',0,False),{'stored_readings':4,'stored_alerts':1,'duplicate_retries':3,'pending_outbox_lines':0})
    def test_retry_is_bounded_and_preserves_pending_records(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'queue';p.write_text('pending\n')
            with patch('replay.flush',return_value=0) as flush,patch('replay.time.sleep') as sleep:
                self.assertEqual(replay_until(p,'http://localhost','key',attempts=3),0)
            self.assertEqual(flush.call_count,3)
            self.assertEqual(sleep.call_count,2)
            self.assertEqual(p.read_text(),'pending\n')
    def test_invalid_retry_options(self):
        for kwargs in [{'attempts':0},{'attempts':True},{'delay':float('nan')}]:
            with self.assertRaises(ValueError): replay_until('unused','unused','unused',**kwargs)
