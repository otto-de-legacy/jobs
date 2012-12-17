#!/bin/bash

VERSION=$1

# download artefact from Nexus
curl -s -S http://nexus.lhotse.ov.otto.de:8080/content/repositories/releases/de/otto/jobs-executor/$VERSION/jobs-executor-$VERSION.zip -o jobs-executor.zip

# extract and prepare
unzip -o jobs-executor.zip
rm jobs-executor.zip
cd jobs-executor

# Assume: zdaemon is already installed

# we execute in our own environment to not mix up anything
virtualenv venv
. venv/bin/activate
pip install --quiet fabric Flask paramiko pycrypto

# TODO configure by provide env variable JOBMONITOR_SETTINGS pointing to file
echo "Starting job executor ..."
nohup python jobmonitor.py &
