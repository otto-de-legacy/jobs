import logging

from flask import Flask, url_for
from flask import json, jsonify
from flask import request, Response

from fabric.api import *

# the user to use for the remote commands
env.user = 'niko'

# the servers where the commands are executed
# TODO: for the time being only the first is used
env.hosts = ['127.0.0.1']

# ------------------------------------------------------
# Control jobs on a remote server 
# ------------------------------------------------------

app = Flask(__name__)

@app.route('/')
def api_root():
    return 'Welcome'


@app.route('/jobs', methods = ['POST'])
def create_job():
    # expect JSON as input
    if request.headers['Content-Type'] != 'application/json':
        return Response("Only 'application/json' currently supported", status=415)

    # Extract Job parameters from JSON
    app.logger.info('create_jobs gather infos from JSON: %s' % request.json)
    job_name = request.json['name']
    if request.json['params']:
        job_params = request.json['params']
        # TODO: Rather use key value pairs
        params_line =  ' '.join([param['val'] for param in job_params])
        #print " LINE: %s" % params_line         
    
    app.logger.info('start triggering job %s ...' % job_name)
    # TODO in cluster environment: for hostname in env.hosts:
    with settings(host_string=env.hosts[0], warn_only=True):
        cmd_result = run("zdaemon -p'%s %s' start fg" % (job_name, params_line))
        app.logger.info('Return code: %d' % cmd_result.return_code)
    
    # construct response
    if cmd_result.failed:
        msg = { 'error': '%s' % cmd_result }
        js = json.dumps(msg)
        resp = Response(js, status=500, mimetype='application/json')
    else:
        # TODO parse output of cmd_result to see whether 
        # ' daemon process already running; pid=...'
        # 'daemon process started, pid=16313'
        msg = { 'output': '%s' % cmd_result }
        js = json.dumps(msg)
        resp = Response(js, status=201, mimetype='application/json')
        resp.headers['Link'] = url_for('get_job_status', job_name=job_name) 
        
    return resp


@app.route('/jobs/<job_name>', methods = ['GET'])
def get_job_status(job_name):

    with settings(host_string=env.hosts[0], warn_only=True):
        cmd_result = run("zdaemon -C%s.conf status" % job_name)
        app.logger.info('Return code: %d' % cmd_result.return_code)

    return 'Status: ' + job_name

# ---------------------------------------
if __name__ == '__main__':
    logging.basicConfig()
    app.run(debug=True)
