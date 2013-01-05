#!/usr/bin/python
"""
   jobmonitor
   ~~~~~~~~~~

   Control jobs on a remote server and expose a HTTP + JSON interface
   to register, start, stop and get status of jobs.

   From the set of registered job templates the user can trigger
   new instances spanned up by zdaemon, only one executed
   at the same time.  Each job instance creates also its
   own log file in the TRANSCRIPT_DIR.
"""

__version__ = "0.8.5"
__author__  = "Niko Schmuck"
__credits__ = ["Ilja Pavkovic", "Sebastian Schroeder"]

import io
import os
import re
import time
import threading
import socket
import logging

from flask import Flask, url_for
from flask import json
from flask import request, Response

from fabric.api import run, settings


# ~~ configuration: override by providing your own settings, see (2)

DEBUG             = True
HTTP_PORT         = 5000
LOGFILE           = 'jobmonitor.log'
TRANSCRIPT_DIR    = '/tmp'
JOB_TEMPLATES_DIR = 'templates'
JOB_INSTANCES_DIR = '/tmp/instances' # IMPORTANT: must be readable by user <JOB_USERNAME>
JOB_HOSTNAME      = 'localhost'
HOSTNAME          = socket.gethostname()

# TODO IMPROVE: make JOB_USERNAME and JOB_HOSTNAME part of the the job definition
if 'jenkins' in HOSTNAME:
    JOB_USERNAME  = 'jenkins'
elif 'fh' in HOSTNAME or 'search' in HOSTNAME:
    JOB_USERNAME  = 'fred'   # TODO: only for the time being, should settings file should be deployed
else:
    JOB_USERNAME  = os.environ['USER']

# ~~ create web application
app = Flask(__name__)
log = logging.getLogger('jobmonitor')

# (1) configure from default settings (see constants of this class)
app.config.from_object(__name__)

# (2) read in configuration file as specified by environment variable
app.config.from_envvar('JOBMONITOR_SETTINGS', silent=True)


# ------------------------------------------------------
# HTTP API
# ------------------------------------------------------

@app.route('/')
def api_root():
    """Index page spitting out who we are."""
    return 'Job Monitor (v %s)' % __version__


@app.route('/jobs', methods = ['GET'])
@app.route('/jobs/', methods = ['GET'])
def get_available_jobs():
    """List all registered jobs known to the system."""
    available_jobs = get_job_template_names()
    msg = { 'jobs': available_jobs }
    return Response(json.dumps(msg), status=200, mimetype='application/json')

@app.route('/jobs/<job_name>', methods = ['POST'])
def create_job(job_name):
    """Register new job (template) in the job monitor."""

    log.info('Going to register %s definition ...', job_name)
    # ~~ expect JSON as input
    definition = request.data
    if not definition:
        return Response("Require job definition as body", status=400)
    if request.headers['Content-Type'] != 'application/text':
        return Response("Only 'application/text' currently supported as media type", status=415)

    if exists_job_template(job_name):
        msg = { 'message': "job '%s' does already exist" % job_name}
        resp = Response(json.dumps(msg), status=303, mimetype='application/json')
    else:
        log.info("Save new job definition for %s ...", job_name)
        save_job_template(job_name, definition)
        resp = Response("", status=201, mimetype='application/json')

    resp.headers['Link'] = url_for('get_current_job', job_name=job_name)
    return resp

@app.route('/jobs/<job_name>', methods = ['DELETE'])
def delete_job(job_name):
    """Unregister job and delete associated template."""

    if exists_job_template(job_name):
        remove_job_template(job_name)
        resp = Response("", status=200, mimetype='application/json')
    else:
        msg = {'message': "No job definition found for %s..." % job_name}
        resp = Response(json.dumps(msg), status=404, mimetype='application/json')

    return resp


@app.route('/jobs/<job_name>', methods = ['GET'])
def get_current_job(job_name):
    if exists_job_instance(job_name):
        job_filepath = get_job_instance_filepath(job_name)
        log.info("Found job instance, check if still running...")
        (job_active, job_process_id) = get_job_status(job_name, job_filepath)
        # TODO: also care for exit code
        return response_job_status(job_name, job_active, 99999, job_process_id)
    else:
        if exists_job_template(job_name):
            msg = { 'message': "No job instance found for '%s'" % job_name }
            return Response(json.dumps(msg), status=200, mimetype='application/json')
        else:
            msg = { 'message': "No job template exists for '%s'" % job_name }
            return Response(json.dumps(msg), status=404, mimetype='application/json')


@app.route('/jobs/<job_name>/<job_id>', methods = ['GET'])
def get_job_by_id(job_name, job_id):
    # check if job exists
    if not exists_job_instance(job_name):
        msg = { 'message': "No job instance found for '%s'" % job_name}
        return Response(json.dumps(msg), status=404, mimetype='application/json')

    job_fullpath = get_job_instance_filepath(job_name)
    (job_active, job_process_id) = get_job_status(job_name, job_fullpath)
    return response_job_status(job_name, job_active, job_id, job_process_id)


@app.route('/jobs/<job_name>/start', methods = ['POST'])
def start_job_instance(job_name):
    """Trigger new job on remote server with given name"""

    log.info('Going to start new %s instance ...', job_name)
    # ~~ expect JSON as input
    if request.headers['Content-Type'] != 'application/json':
        return Response("Only 'application/json' currently supported as media type", status=415)

    # ~~ check if already running
    job_active = False
    job_process_id = -1
    if exists_job_instance(job_name):
        log.info("Found job instance, check if still running...")
        job_filepath = get_job_instance_filepath(job_name)
        (job_active, job_process_id) = get_job_status(job_name, job_filepath)

    if job_active:
        log.info('Job %s is still active, link to existing instance ...', job_name)
        job_id = 99999
        msg = { 'status': 'RUNNING', 'message': "job '%s' is still running with process id %d" % (job_name, job_process_id)}
        resp = Response(json.dumps(msg), status=303, mimetype='application/json')
        resp.headers['Link'] = url_for('get_job_by_id', job_name=job_name, job_id=job_id)
    else:
        log.info('Job %s is not active, going to start new instance ...', job_name)
        # ~~ extract Job parameters from JSON and create job config
        job_id = create_job_id()
        job_params = request.json['parameters'] if request.json['parameters'] else {}

        log.info('preparing job %s with params: %s' % (job_name, job_params))
        job_filepath = create_jobconf(job_name, job_id, job_params)

        # ~~ going to start of daemonized process
        log.info('trying to start job %s ...' % job_name)
        # TODO in cluster environment: for hostname in env.hosts
        with settings(host_string=app.config['JOB_HOSTNAME'], user=app.config['JOB_USERNAME'], warn_only=True):
            cmd_result = run("zdaemon -C %s start" % job_filepath)
            log.info('Started %s [return code: %d]' % (job_name, cmd_result.return_code))

        # ~~ construct response
        if cmd_result.succeeded and "daemon process started" in cmd_result:
            job_process_id = extract_process_id(cmd_result)
            msg = { 'status': 'STARTED', 'message': "job '%s' started with process id=%d" % (job_name, job_process_id) }
            resp = Response(json.dumps(msg), status=201, mimetype='application/json')
            resp.headers['Link'] = url_for('get_job_by_id', job_name=job_name, job_id=job_id)
        else:
            msg = { 'status': 'FINISHED', 'result': {'ok': False, 'message': '%s' % cmd_result,
                                                     'exit_code': cmd_result.return_code }}
            resp = Response(json.dumps(msg), status=500, mimetype='application/json')

    return resp


@app.route('/jobs/<job_name>/stop', methods = ['POST'])
@app.route('/jobs/<job_name>/<job_id>/stop', methods = ['POST'])  # DEPRECATED
def stop_job_instance(job_name, job_id):
    # check if job exists
    if not exists_job_instance(job_name):
        return Response("No job instance found for '%s'" % job_name, status=404)

    # check if job is active
    job_fullpath = get_job_instance_filepath(job_name)
    (job_active, job_process_id) = get_job_status(job_name, job_fullpath)
    if not job_active:
        msg = { 'status': 'FINISHED', 'message':'job has already finished' }
        return Response(json.dumps(msg), status=403, mimetype='application/json')
    else:
        with settings(host_string=app.config['JOB_HOSTNAME'], user=app.config['JOB_USERNAME'], warn_only=True):
            cmd_result = run("zdaemon -C %s stop" % job_fullpath)
            log.info('Return code from stop job %s: %d' % (job_name, cmd_result.return_code))

        if cmd_result.succeeded and "daemon process stopped" in cmd_result:
            msg = { 'status': 'FINISHED', 'result': {'ok': True, 'message': "job '%s' stopped with process id=%d" % (job_name, job_process_id) } }
            return Response(json.dumps(msg), status=200, mimetype='application/json')
        else:
            msg = { 'status': 'ERROR', 'result': {'ok': False, 'message': "%s" % cmd_result } }
            return Response(json.dumps(msg), status=500, mimetype='application/json')


# ------------------------------------------------------

def response_job_status(job_name, job_active, job_id, job_process_id):
    log_lines = get_last_lines(get_job_instance_logpath(job_name, job_id), 100)
    if job_active:
        msg = { 'status': 'RUNNING', 'job_id': "%s" % job_id, 'log_lines': log_lines,
                'message': "job '%s' is running with process id %d" % (job_name, job_process_id)}
        return Response(json.dumps(msg), status=200, mimetype='application/json')
    else:
        msg = { 'status': 'FINISHED', 'log_lines': log_lines,
                'result':{'ok': True, 'message': "job '%s' is not running any more" % job_name }}
        return Response(json.dumps(msg), status=200, mimetype='application/json')

def get_job_status(job_name, job_filepath):
    job_active = False
    job_process_id = -1
    with settings(host_string=app.config['JOB_HOSTNAME'], user=app.config['JOB_USERNAME'], warn_only=True):
        cmd_result = run("zdaemon -C %s status" % job_filepath)
        if cmd_result.return_code > 0:
            log.warn("Problem while checking job status: %s" % cmd_result)
        else:
            job_active = cmd_result.succeeded and "program running" in cmd_result
            if job_active:
                job_process_id = extract_process_id(cmd_result)
            log.info('%s running? %s [pid=%d]' % (job_name, job_active, job_process_id))
    return job_active, job_process_id


def create_jobconf(job_name, job_id, params):
    # create new instance file for writing
    file_path = get_job_instance_filepath(job_name)
    ensure_job_instance_directory()
    instance_file = io.open(file_path, 'w')
    # add standard values to replace dictionary
    params['transcript_file'] = get_job_instance_logpath(job_name, job_id)

    # read the lines from the template, substitute the values, and write to the instance
    for line in io.open(get_job_template_filepath(job_name), 'r'):
        for (key, value) in params.items():
            line = line.replace('$'+key, value)
        instance_file.write(line)

    instance_file.close()
    return os.path.abspath(file_path)

# ------------------------------------------------------

def get_job_template_filename(job_name):
    return "%s.conf" % job_name

def get_job_template_filepath(job_name):
    return os.path.join(app.config['JOB_TEMPLATES_DIR'], get_job_template_filename(job_name))

def exists_job_template(job_name):
    return os.path.exists(get_job_template_filepath(job_name))

def save_job_template(job_name, data):
    """Saves the given data to a job template definition file."""
    fullpath = get_job_template_filepath(job_name)
    file = open(fullpath, 'w')
    file.write(make_multiline_conf(data))

def make_multiline_conf(line):
    """A bit of a hack: make sure the zdaemon definition is split on multiple lines."""
    trans1 = re.sub(r'<(/?\w+)>', r'\n<\1>\n', line)
    return re.sub(r'(transcript )', r'\n\1', trans1)

def remove_job_template(job_name):
    fullpath = get_job_template_filepath(job_name)
    os.remove(fullpath)

def get_job_template_names():
    templates_dir = app.config['JOB_TEMPLATES_DIR']
    names = []
    for filename in os.listdir(templates_dir):
        (basename, ext) = os.path.splitext(filename)
        if os.path.isfile(os.path.join(templates_dir, filename)) and ext == '.conf':
            names.append(basename)
    return names

def create_job_id():
    """Generate ID which can be used to uniquely refer to a job instance"""
    return time.strftime("%Y-%m-%d_%H%M%S")

# def get_job_instance(job_name, job_id):
#    return "%s_%s" % (job_name, job_id)

def get_job_instance_filename(job_name):
    return "%s.conf" % job_name

def get_job_instance_logname(job_name, job_id):
    return "%s_%s.log" % (job_name, job_id)

def get_job_instance_logpath(job_name, job_id):
    return os.path.join(app.config['TRANSCRIPT_DIR'], get_job_instance_logname(job_name, job_id))

def get_job_instance_filepath(job_name):
    abs_instances_dir = os.path.abspath(app.config['JOB_INSTANCES_DIR'])
    return os.path.join(abs_instances_dir, get_job_instance_filename(job_name))

def ensure_job_instance_directory():
    if not os.path.exists(app.config['JOB_INSTANCES_DIR']):
        os.makedirs(app.config['JOB_INSTANCES_DIR'])

def exists_job_instance(job_name):
    return os.path.exists(get_job_instance_filepath(job_name))


def extract_process_id(cmd_string):
    matcher = re.search(r'pid=(\d+)', cmd_string)
    if matcher:
        return int(matcher.group(1))
    else:
        return -1

def get_last_lines(filepath, nr_lines):
    """Return the last nr_lines from file (as given by filepath)."""
    if os.path.exists(filepath):
        fh = open(filepath, 'r')
        file_size = fh.tell()
        fh.seek(max(file_size - 4*1024, 0))
        # this will get rid of trailing newlines, unlike readlines()
        return fh.read().splitlines()[-nr_lines:]
    else:
        return []

def remove_old_files(dir, days):
    """Delete files older than specified days from file system."""
    log.info("Clean up old files in %s ..." % dir)
    now = time.time()
    for filename in os.listdir(dir):
        fullpath = os.path.join(dir, filename)
        if os.stat(fullpath).st_mtime < now - days * 86400:
            os.remove(os.path.join(dir, filename))

def permanent_check():
    remove_old_files(app.config['JOB_INSTANCES_DIR'], 3) # days
    # execute every ... minutes
    threading.Timer(15 * 60, permanent_check).start()


# ---------------------------------------

if __name__ == '__main__':

    # configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    app.run(host='0.0.0.0', port=HTTP_PORT, threaded=True)
    permanent_check()
