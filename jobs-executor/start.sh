#!/bin/bash
cd "$(dirname "$0")"

# we execute in our own environment to not mix up anything
virtualenv venv
. venv/bin/activate

# TODO configure by provide env variable JOBMONITOR_SETTINGS pointing to file
echo "Starting job executor ..."
python jobmonitor.py
