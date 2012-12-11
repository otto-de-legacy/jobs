#!/usr/bin/python

import io
import os
import logging

from flask import Flask, url_for
from flask import json
from flask import request, Response

from fabric.api import *

__version__ = "0.1"

# the user to use for the remote commands
env.user = 'nschmuck'

# the servers where the commands are executed
# TODO: for the time being only the first is used
env.hosts = ['abraxas']

# TODO: allow to configure own directory with job definitions to decouple

# ------------------------------------------------------

def get_job_instance(job_name, job_id):
    return "%s_%s" % (job_name, job_id)

def get_job_instance_filename(job_name, job_id):
    return "%s.conf" % get_job_instance(job_name, job_id)

def get_job_instance_filepath(job_name, job_id):
    file_path = "instances/%s" % get_job_instance_filename(job_name, job_id)
    return os.path.abspath(file_path)

def exists_job_instance(job_name, job_id):
    return os.path.exists(get_job_instance_filepath(job_name, job_id))

def get_job_template_filename(job_name):
    return "%s.conf" % job_name

def get_job_template_filepath(job_name):
    file_path = "templates/%s" % get_job_template_filename(job_name)
    return os.path.abspath(file_path)

def get_job_template_names():
    template_path = os.path.abspath('templates')
    names = []
    for cur_file in os.listdir(template_path):
        (basename, ext) = os.path.splitext(cur_file)
        if os.path.isfile(os.path.join(template_path, cur_file)) and  ext == '.conf':
            names.append(basename)
    return names


def create_jobconf(job_id, job_name, params):
    file_path = get_job_instance_filepath(job_name, job_id)
    # create new config file for writing
    instance_file = io.open(file_path, 'w')

    # read the lines from the template, substitute the values, and write to the new config file
    for line in io.open(get_job_template_filepath(job_name), 'r'):
        for (key, value) in params.items():
            line = line.replace('$'+key, value)
        instance_file.write(line)

    instance_file.close()
    return file_path


# ------------------------------------------------------
# Control jobs on a remote server 
# ------------------------------------------------------

app = Flask(__name__)

@app.route('/')
def api_root():
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
    with settings(host_string=env.hosts[0], warn_only=True):
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
        log_output = get("demojob.log")
        # TODO: how to only get the 'delta' messages, happened since last call?
        app.logger.info('LOG---> ' + str(log_output))
        app.logger.info('Return code: %d' % cmd_result.return_code)

    return 'Status: ' + cmd_result


@app.route('/jobs/<job_name>/<job_id>', methods = ['DELETE'])
def kill_job(job_name, job_id):

    # check if job exists
    if not exists_job_instance(job_name, job_id):
        return Response("No job instance '%s' found for '%s'" % (job_id, job_name), status=404)

    with settings(host_string=env.hosts[0], warn_only=True):
        cmd_result = run("zdaemon -C%s stop" % get_job_instance_filepath(job_name, job_id))
        app.logger.info('Return code: %d' % cmd_result.return_code)

    return 'Status: ' + cmd_result


# ---------------------------------------
if __name__ == '__main__':
    logging.basicConfig()
    app.run(debug=True)
