import os
import jobmonitor
import unittest
import tempfile

class JobMonitorTestCase(unittest.TestCase):

    def setUp(self):
        self.db_fd, jobmonitor.app.config['DATABASE'] = tempfile.mkstemp()
        jobmonitor.app.config['TESTING'] = True
        self.app = jobmonitor.app.test_client()

    def tearDown(self):
        os.close(self.db_fd)
        os.unlink(jobmonitor.app.config['DATABASE'])

    def test_root_url(self):
        rv = self.app.get('/')
        assert 'Job Monitor' in rv.data

if __name__ == '__main__':
    unittest.main()