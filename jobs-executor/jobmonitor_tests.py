#!/usr/bin/python
"""
   Tests for the jobmonitor web application.
"""

import os
import unittest

import flask
from flask.helpers import json
from auto_stub import TestCase

import jobmonitor

def disabled(f):
    def _decorator(no):
        print f.__name__ + ' has been disabled'
    return _decorator


class JobMonitorUnitTests(TestCase):

    def test_create_jobconf(self):
        fullpath = jobmonitor.create_jobconf('demojob', '4711', {'sample_file':'/var/log/syslog','key2':'var2'})
        self.assertTrue(len(fullpath) > 0)

    def test_job_id(self):
        job_id = jobmonitor.create_job_id()
        self.assertEqual(len(job_id), 17)

    def test_extract_valid_process_id(self):
        pid = jobmonitor.extract_process_id("pid=4711")
        self.assertEqual(pid, 4711)

    def test_extract_invalid_process_id(self):
        pid = jobmonitor.extract_process_id("pid=A711")
        self.assertEqual(pid, None)

class JobMonitorIntegrationTests(TestCase):

    def setUp(self):
        jobmonitor.app.config['TESTING'] = True
        # get rid of old job instance file
        try:
            os.remove(os.path.join(jobmonitor.app.config['JOB_INSTANCES_DIR'], 'demojob.conf'))
            os.remove(os.path.expanduser('~/zdsock'))
        except OSError:
            pass
        # prepare HTTP client
        self.app = jobmonitor.app.test_client()
        print "starting test using: %s" % jobmonitor.app.config
        # if there is still a running demojob instance, stop it first
        rv = self.app.get('/jobs/demojob')
        self.assertLess(rv.status_code, 500)
        if rv.status_code == 200:
            resp_js = flask.json.loads(rv.data)
            self.assertTrue(resp_js.has_key('message'), "No message found in JSON response")
            self.assertTrue(resp_js['message'], "No job instance found for 'demojob'")
        # delete old test job
        rv_delete = self.app.delete('/jobs/test_job')
        self.assertLess(rv_delete.status_code, 500)


    def tearDown(self):
        pass

    def test_root_url(self):
        rv = self.app.get('/')
        self.assertEqual(200, rv.status_code)
        self.assertIn('Job Monitor', rv.data)

    def test_redirect_get_jobs(self):
        rv = self.app.get('/jobs')
        self.assertEqual(301, rv.status_code)

    def test_get_available_jobs(self):
        rv = self.app.get('/jobs/')
        self.assertEqual(200, rv.status_code)
        self.assertEqual('application/json', rv.headers['Content-Type'])
        self.assertIn('demojob', rv.data)

    def test_get_unknown_job(self):
        rv = self.app.get('/jobs/foobar')
        self.assertEqual(404, rv.status_code)
        self.assertEqual('application/json', rv.headers['Content-Type'])
        self.assertIn("No job template exists for 'foobar'", rv.data)

    def test_get_unknown_job_instance(self):
        rv = self.app.get('/jobs/foobar/4711')
        self.assertEqual(404, rv.status_code)
        self.assertEqual('application/json', rv.headers['Content-Type'])
        self.assertIn("No job instance found for 'foobar'", rv.data)

    def test_create_new_empty_job(self):
        rv = self.app.post('/jobs/test_job', content_type='application/text')
        self.assertEqual(400, rv.status_code)

    def test_create_new_job(self):
        payload = open('tests/test_job.conf', 'r').read()
        rv = self.app.post('/jobs/test_job', data=payload, content_type='application/text')
        self.assertEqual(201, rv.status_code)
        self.assertEqual('application/json', rv.headers['Content-Type'])
        job_url = rv.headers["Link"]
        self.assertTrue(os.path.exists("templates/test_job.conf"), "Template was not uploaded properly")
        # try to follow link
        rv_get = self.app.get(job_url)
        self.assertEqual(200, rv_get.status_code, "Link '%s' cannot be resolved" % job_url)

    def test_start_new_job_two_times(self):
        payload = open('tests/test_job.conf', 'r').read()
        rv = self.app.post('/jobs/test_job', data=payload, content_type='application/text')
        self.assertEqual(201, rv.status_code)
        rv2 = self.app.post('/jobs/test_job', data=payload, content_type='application/text')
        self.assertEqual(303, rv2.status_code, "Job 'test_job' should already exist")

    def test_start_job_instance_missing_parameter(self):
        payload = { 'parameters': { "key1": "val1"} }
        rv = self.app.post('/jobs/demojob/start', content_type='application/json', data=json.dumps(payload))
        self.assertEqual(500, rv.status_code)
        self.assertEqual('application/json', rv.headers['Content-Type'])
        self.assertIn('Error: no replacement for \'sample_file\'', rv.data)

    def test_start_job_instance_missing_parameter(self):
        rv = self.app.post('/jobs/demojob/start', content_type='text/html', data="<body>foobar</body>")
        self.assertEqual(415, rv.status_code)

    def test_start_job_instance_successfull(self):
        payload = { 'parameters': { "sample_file": "/var/log/mongodb.log" } }
        rv = self.app.post('/jobs/demojob/start', content_type='application/json', data=json.dumps(payload))
        self.assertEqual(201, rv.status_code)
        self.assertEqual('application/json', rv.headers['Content-Type'])
        self.assertIn('job \'demojob\' started with process id=', rv.data)
        rv_stop = self.app.post('/jobs/demojob/stop')
        self.assertEqual(200, rv_stop.status_code)

    def test_start_job_instance_two_times(self):
        payload = { 'parameters': { "sample_file": "/var/log/mongodb.log" } }
        rv = self.app.post('/jobs/demojob/start', content_type='application/json', data=json.dumps(payload))
        self.assertEqual(201, rv.status_code)
        rv = self.app.post('/jobs/demojob/start', content_type='application/json', data=json.dumps(payload))
        self.assertEqual(303, rv.status_code)
        self.assertEqual('application/json', rv.headers['Content-Type'])
        resp_js = flask.json.loads(rv.data)
        self.assertIn('RUNNING', resp_js['status'])

    def test_get_job_status(self):
        payload = { 'parameters': { "sample_file": "/var/log/mongodb.log" } }
        rv = self.app.post('/jobs/demojob/start', content_type='application/json', data=json.dumps(payload))
        self.assertEqual(201, rv.status_code)
        # Follow link as per respsone header
        job_url = rv.headers["Link"]
        rv_get = self.app.get(job_url)
        self.assertEqual(200, rv_get.status_code)

    def test_stop_job_two_times(self):
        payload = { 'parameters': { "sample_file": "/var/log/mongodb.log" } }
        rv = self.app.post('/jobs/demojob/start', content_type='application/json', data=json.dumps(payload))
        resp_js = flask.json.loads(rv.data)
        self.assertIn('STARTED', resp_js['status'])
        job_url = rv.headers["Link"]
        rv_stop = self.app.post('%s/stop' % job_url)
        self.assertEqual(200, rv_stop.status_code)
        resp_js = flask.json.loads(rv_stop.data)
        self.assertIn('FINISHED', resp_js['status'])
        # try to stop second time...
        rv_stop = self.app.post('%s/stop' % job_url)
        self.assertEqual(403, rv_stop.status_code)
        self.assertIn('FINISHED', resp_js['status'])

# --
if __name__ == '__main__':
    unittest.main()