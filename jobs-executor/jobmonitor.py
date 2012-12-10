#!/usr/bin/python

import io
import os, binascii
import logging

from flask import Flask, url_for
from flask import json
from flask import request, Response

from fabric.api import *

# the user to use for the remote commands
env.user = 'nschmuck'

# the servers where the commands are executed
# TODO: for the time being only the first is used
env.hosts = ['abraxas']

#env.reject_unknown_hosts = False
#env.disable_known_hosts = True

#env.no_keys = True
#env.host_string = 'nschmuck@localhost'

# ------------------------------------------------------

def create_jobconf(job_name, params):
    # create random ascii string for identifying job
    rnd = binascii.b2a_hex(os.urandom(6))
    job_instance = "%s_%s" % (job_name, rnd)
    file_name = "instances/%s.conf" % job_instance
    # create new config file for writing
    config_file = io.open(file_name, 'w')

    # read the lines from the template, substitute the values, and write to the new config file
    for line in io.open("templates/%s.conf" % job_name, 'r'):
        for (key, value) in params.items():
            line = line.replace('$'+key, value)
        config_file.write(line)

    config_file.close()
    return (job_instance, os.path.abspath(file_name))


# ------------------------------------------------------
# Control jobs on a remote server 
# ------------------------------------------------------

app = Flask(__name__)

@app.route('/')
def api_root():
    return 'Job Monitor'


@app.route('/jobs', methods = ['GET'])
def get_available_jobs():
    job_files = []
    for cur_file in os.listdir('templates'):
        if os.path.isfile(cur_file):
            job_files.append(cur_file)
    return job_files


@app.route('/jobs', methods = ['POST'])
def create_job():
    # expect JSON as input
    if request.headers['Content-Type'] != 'application/json':
        return Response("Only 'application/json' currently supported", status=415)

    # TODO: check if already running
    # Extract Job parameters from JSON
    job_name = request.json['name']
    job_params = request.json['params']

    app.logger.info('preparing job %s with params: %s' % (job_name, job_params))
    (job_instance, job_filename) = create_jobconf(job_name, job_params)
    
    app.logger.info('trying to start job %s ...' % job_name)
    # TODO in cluster environment: for hostname in env.hosts:
    with settings(host_string=env.hosts[0], warn_only=True):
        cmd_result = local("zdaemon -C%s start" % job_filename)
        app.logger.info('Return code: %d' % cmd_result.return_code)
        app.logger.info('Result: %s' % cmd_result)

    # construct response
    if cmd_result.failed:
        msg = { 'error': '%s' % cmd_result }
        js = json.dumps(msg)
        resp = Response(js, status=500, mimetype='application/json')
    else:
        if 'daemon process already running' in cmd_result:
            msg = { 'output': '%s' % cmd_result }
            js = json.dumps(msg)
            resp = Response(js, status=303, mimetype='application/json')
            os.remove(job_filename)
        else:
            # TODO parse output of cmd_result to see whether 
            # 'daemon process already running; pid=...'
            # 'daemon process started, pid=16313'
            msg = { 'output': '%s' % cmd_result }
            js = json.dumps(msg)
            resp = Response(js, status=201, mimetype='application/json')
            resp.headers['Link'] = url_for('get_job_status', job_instance=job_instance) 
        
    return resp


@app.route('/jobs/<job_instance>', methods = ['GET'])
def get_job_status(job_instance):

    job_filename = os.path.abspath('instances/%s.conf' % job_instance)
    # check if file exists
    if not os.path.exists(job_filename):
        return Response("No job instance with name '%s' found" % job_instance, status=404) 

    with settings(host_string=env.hosts[0], warn_only=True):
        cmd_result = run("zdaemon -C%s status" % job_filename)
        log_output = get("demojob.log")
        # TODO: how to only get the 'delta' messages, happened since last call?
        app.logger.info('LOG---> ' + str(log_output))
        app.logger.info('Return code: %d' % cmd_result.return_code)

    return 'Status: ' + cmd_result

@app.route('/jobs/<job_instance>', methods = ['DELETE'])
def kill_job(job_instance):

    job_filename = os.path.abspath('instances/%s.conf' % job_instance)
    # check if file exists
    if not os.path.exists(job_filename):
        return Response("No job instance with name '%s' found" % job_instance, status=404) 

    with settings(host_string=env.hosts[0], warn_only=True):
        cmd_result = run("zdaemon -C%s stop" % job_filename)
        app.logger.info('Return code: %d' % cmd_result.return_code)

    return 'Status: ' + cmd_result

# ---------------------------------------
if __name__ == '__main__':
    logging.basicConfig()
    app.run(debug=True)
