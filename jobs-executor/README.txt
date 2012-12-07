------------
Requirements
------------

* sudo apt-get install python-pip
* sudo apt-get install fabric
* sudo pip install Flask



Start local HTTP server with:

python jobmonitor.py

... should now listen on port 5000

curl -X POST -d "name=asdhk" http://127.0.0.1:5000/jobs?huh=heinz
zdaemon -v
curl -i -X POST -H'Content-Type: application/json' -d '{"name":"ls"}' http://127.0.0.1:5000/jobs

curl -i   http://127.0.0.1:5000/jobs/testjob
curl -i -X POST -H'Content-Type: application/json'  http://127.0.0.1:5000/jobs -d @demojob.json
