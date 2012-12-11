#!/usr/bin/python
"""
   jobmonitor
   ~~~~~~~~~~

   Control jobs on a remote server and expose a small
   JSON HTTP interface to start, stop and get status.
"""
__version__ = "0.1"


import io
import os
import logging

from flask import Flask, url_for
from flask import json
from flask import request, Response

from fabric.api import *

# ~~ configuration (override by providing your own settings, see below)

DEBUG = True
LOGFILE = 'jobmonitor.log'
JOB_TEMPLATES_DIR = 'templates'
JOB_INSTANCES_DIR = 'instances'
JOB_HOSTNAME = 'localhost'
#JOB_USERNAME = '...'


# ~~ create web application
app = Flask(__name__)

# (1) configure from default settings (see constants of this class)
app.config.from_object(__name__)

# (2) read in configuration file as specified by environment variable
app.config.from_envvar('JOBMONITOR_SETTINGS', silent=True)


# ------------------------------------------------------

@app.route('/')
def api_root():
    app.logger.info("START")
    return 'Job Monitor (v %s)' % __version__


@app.route('/jobs', methods = ['GET'])
def get_available_jobs():
    available_jobs = get_job_template_names()
    msg = { 'jobs': available_jobs }
    js = json.dumps(msg)
    return Response(js, status=200, mimetype='application/json')


@app.route('/jobs/<job_name>', methods = ['POST'])
def create_job(job_name):
    # ~~ expect JSON as input
    if request.headers['Content-Type'] != 'application/json':
        return Response("Only 'application/json' currently supported as media type", status=415)

    # TODO: check if already running
    # ~~ extract Job parameters from JSON and create job config
    job_id = request.json['client_id']
    job_params = request.json['parameters']

    app.logger.info('preparing job %s with params: %s' % (job_name, job_params))
    job_filename = create_jobconf(job_id, job_name, job_params)

    # ~~ going to start of daemonized process
    app.logger.info('trying to start job %s ...' % job_name)
    # TODO in cluster environment: for hostname in env.hosts:
    with settings(host_string=app.config['JOB_HOSTNAME'], warn_only=True):
        cmd_result = run("zdaemon -C%s start" % job_filename)
        app.logger.info('Return code: %d' % cmd_result.return_code)

    # ~~ construct response
    if cmd_result.failed:
        msg = { 'error': '%s' % cmd_result }
        js = json.dumps(msg)
        resp = Response(js, status=500, mimetype='application/json')
    else:
        if 'daemon process already running' in cmd_result:
            msg = { 'output': '%s' % cmd_result }
            js = json.dumps(msg)
            resp = Response(js, status=303, mimetype='application/json')
        else:
            # TODO parse output of cmd_result to see whether
            # 'daemon process already running; pid=...'
            # 'daemon process started, pid=16313'
            # . Unlinking stale socket /home/nschmuck/zdsock; sleep 1\r\n. \r\ndaemon process started, pid=19242
            msg = { 'output': '%s' % cmd_result }
            js = json.dumps(msg)
            resp = Response(js, status=201, mimetype='application/json')
            resp.headers['Link'] = url_for('get_job_status', job_name=job_name, job_id=job_id)
        
    return resp


@app.route('/jobs/<job_name>/<job_id>', methods = ['GET'])
def get_job_status(job_name, job_id):

    # check if job exists
    if not exists_job_instance(job_name, job_id):
        return Response("No job instance '%s' found for '%s'" % (job_id, job_name), status=404)

    with settings(host_string=env.hosts[0], warn_only=True):
        cmd_result = run("zdaemon -C%s status" % get_job_instance_filepath(job_name, job_id))
        # via fabric: log_output = get("demojob.log")
        app.logger.info('Return code: %d' % cmd_result.return_code)

    msg = { 'status': '%s' % cmd_result }
    js = json.dumps(msg)
    return Response(js, status=200, mimetype='application/json')


@app.route('/jobs/<job_name>/<job_id>', methods = ['DELETE'])
def kill_job(job_name, job_id):

    # check if job exists
    if not exists_job_instance(job_name, job_id):
        return Response("No job instance '%s' found for '%s'" % (job_id, job_name), status=404)

    with settings(host_string=env.hosts[0], warn_only=True):
        cmd_result = run("zdaemon -C%s stop" % get_job_instance_filepath(job_name, job_id))
        app.logger.info('Return code: %d' % cmd_result.return_code)

    msg = { 'status': '%s' % cmd_result }
    js = json.dumps(msg)
    return Response(js, status=200, mimetype='application/json')


# ------------------------------------------------------

def get_job_instance(job_name, job_id):
    return "%s_%s" % (job_name, job_id)

def get_job_instance_filename(job_name, job_id):
    return "%s.conf" % get_job_instance(job_name, job_id)

def get_job_instance_filepath(job_name, job_id):
    return os.path.join(app.config['JOB_INSTANCES_DIR'], get_job_instance_filename(job_name, job_id))

def ensure_job_instance_directory():
    if not os.path.exists(app.config['JOB_INSTANCES_DIR']):
        os.makedirs(app.config['JOB_INSTANCES_DIR'])

def exists_job_instance(job_name, job_id):
    return os.path.exists(get_job_instance_filepath(job_name, job_id))

def get_job_template_filename(job_name):
    return "%s.conf" % job_name

def get_job_template_filepath(job_name):
    return os.path.join(app.config['JOB_TEMPLATES_DIR'], get_job_template_filename(job_name))

def get_job_template_names():
    template_path = app.config['JOB_TEMPLATES_DIR']
    names = []
    for cur_file in os.listdir(template_path):
        (basename, ext) = os.path.splitext(cur_file)
        if os.path.isfile(os.path.join(template_path, cur_file)) and  ext == '.conf':
            names.append(basename)
    return names

def create_jobconf(job_id, job_name, params):
    # create new instance file for writing
    ensure_job_instance_directory()
    file_path = get_job_instance_filepath(job_name, job_id)
    instance_file = io.open(file_path, 'w')

    # read the lines from the template, substitute the values, and write to the instance
    for line in io.open(get_job_template_filepath(job_name), 'r'):
        for (key, value) in params.items():
            line = line.replace('$'+key, value)
        instance_file.write(line)

    instance_file.close()
    return os.path.abspath(file_path)


# ---------------------------------------
if __name__ == '__main__':

    # ~~ configure logging for production
    if not app.debug:
        file_handler = logging.FileHandler(filename=app.config['LOGFILE'])
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        app.logger.addHandler(file_handler)

    app.logger.info("Going to start jobmonitor ...")
    app.run()
