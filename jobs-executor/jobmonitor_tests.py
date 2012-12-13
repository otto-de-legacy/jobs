#!/usr/bin/python
"""
   Tests for the jobmonitor web application.
"""

import flask
from flask.helpers import json
import jobmonitor
import unittest

class JobMonitorTestCase(unittest.TestCase):

    def setUp(self):
        jobmonitor.app.config['TESTING'] = True
        self.app = jobmonitor.app.test_client()
        # if running job instance, kill it first
        rv = self.app.get('/jobs/demojob')
        self.assertLess(rv.status_code, 500)
        if rv.status_code == 200:
            resp_js = flask.json.loads(rv.data)
            if resp_js.has_key('job_id'):
                job_id = resp_js['job_id']
                rv_stop = self.app.post('/jobs/demojob/%s/stop' % job_id)
                self.assertEqual(200, rv_stop.status_code)

    def tearDown(self):
        pass

    def test_root_url(self):
        rv = self.app.get('/')
        self.assertEqual(200, rv.status_code)
        self.assertIn('Job Monitor', rv.data)

    def test_get_available_jobs(self):
        rv = self.app.get('/jobs')
        self.assertEqual(200, rv.status_code)
        self.assertEqual('application/json', rv.headers['Content-Type'])
        self.assertIn('demojob', rv.data)

    def test_get_unknown_job(self):
        rv = self.app.get('/jobs/foobar')
        self.assertEqual(404, rv.status_code)
        self.assertEqual('application/json', rv.headers['Content-Type'])
        self.assertIn("No job instance found for 'foobar'", rv.data)

    def test_get_unknown_job_instance(self):
        rv = self.app.get('/jobs/foobar/4711')
        self.assertEqual(404, rv.status_code)
        self.assertEqual('application/json', rv.headers['Content-Type'])
        self.assertIn("No job instance '4711' found for 'foobar'", rv.data)

    def test_create_new_job_(self):
        payload = open('tests/test_job.conf', 'r').read()
        rv = self.app.post('/jobs/test_job', data=payload)
        self.assertEqual(201, rv.status_code)
        self.assertEqual('application/json', rv.headers['Content-Type'])
        job_url = rv.headers["Link"]
        rv_get = self.app.get(job_url)
        self.assertEqual(200, rv_get.status_code)
        rv_delete = self.app.delete(job_url)
        self.assertEqual(200, rv_delete.status_code)

    def test_create_job_instance_missing_parameter(self):
        payload = { 'parameters': { "key1": "val1"} }
        rv = self.app.post('/jobs/demojob/start', content_type='application/json', data=json.dumps(payload))
        self.assertEqual(500, rv.status_code)
        self.assertEqual('application/json', rv.headers['Content-Type'])
        self.assertIn('Error: no replacement for \'sample_file\'', rv.data)

    def test_create_job_instance_successfull(self):
        payload = { 'parameters': { "sample_file": "/var/log/syslog" } }
        rv = self.app.post('/jobs/demojob/start', content_type='application/json', data=json.dumps(payload))
        self.assertEqual(201, rv.status_code)
        self.assertEqual('application/json', rv.headers['Content-Type'])
        self.assertIn('job \'demojob\' started with process id=', rv.data)

    def test_get_job_instance(self):
        payload = { 'parameters': { "sample_file": "/var/log/syslog" } }
        rv = self.app.post('/jobs/demojob/start', content_type='application/json', data=json.dumps(payload))
        self.assertEqual(201, rv.status_code)
        # Follow link as per respsone header
        job_url = rv.headers["Link"]
        rv_get = self.app.get(job_url)
        self.assertEqual(200, rv_get.status_code)



if __name__ == '__main__':
    unittest.main()