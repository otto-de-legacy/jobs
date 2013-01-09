#!/bin/sh
cd jobs-executor

scp prepare.sh fred@$server:.
ssh fred@$server "./prepare.sh $version"

scp start.sh fred@$server:jobs-executor/.
ssh -n -f fred@$server "sh -c 'cd jobs-executor; nohup ./start.sh > /tmp/jobmonitor.log 2>&1 &'"

sleep 5
uname -a
id
curl -s -S -i http://$server:5000/
echo ""