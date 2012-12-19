#!/bin/bash
cd "$(dirname "$0")"

# we execute in our own environment to not mix up anything
virtualenv venv
. venv/bin/activate

# TODO configure by provide env variable JOBMONITOR_SETTINGS pointing to file
echo "Starting job executor ..."

# TODO improve by using gunicorn (see http://flask.pocoo.org/docs/deploying/others/)
#   gunicorn -b 0.0.0.0:5000 jobmonitor:app

python jobmonitor.py
