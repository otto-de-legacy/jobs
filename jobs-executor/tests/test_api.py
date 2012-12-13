import os
import jobmonitor
import unittest
import tempfile

class JobMonitorTestCase(unittest.TestCase):

    def setUp(self):
        self.db_fd, jobmonitor.app.config['DATABASE'] = tempfile.mkstemp()
        jobmonitor.app.config['TESTING'] = True
        self.app = jobmonitor.app.test_client()
        jobmonitor.start_job_instance("demojob")

    def tearDown(self):
        os.close(self.db_fd)
        os.unlink(jobmonitor.app.config['DATABASE'])

if __name__ == '__main__':
    unittest.main()