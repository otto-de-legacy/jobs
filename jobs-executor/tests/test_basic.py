import unittest
import jobmonitor
import uuid

class TestJobMonitorFunctions(unittest.TestCase):

    def test_shuffle(self):
        jobmonitor.create_jobconf('demojob', '4711', {'sample_file':'/var/log/syslog','key2':'var2'})
        self.assertEqual(9, 9)

    def test_unique_id(self):
        rand_id = uuid.uuid4()
        print "Random: %s" % rand_id


if __name__ == '__main__':
    unittest.main()