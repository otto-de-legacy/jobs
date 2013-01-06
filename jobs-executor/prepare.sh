#!/bin/bash

VERSION=$1

# download artefact from Nexus
curl -s -S http://nexus.lhotse.ov.otto.de:8080/content/repositories/releases/de/otto/jobs-executor/$VERSION/jobs-executor-$VERSION.zip -o jobs-executor.zip

# extract and prepare
unzip -o jobs-executor.zip
rm jobs-executor.zip
cd jobs-executor
mkdir -p instances

# Stop old jobmonitor process
kill $(ps aux | grep '[p]ython jobmonitor.py' | awk '{print $2}')

# Assume: zdaemon is already installed
# we execute in our own environment to not mix up anything
virtualenv venv
. venv/bin/activate
pip install --quiet fabric Flask paramiko pycrypto ordereddict

exit 0
