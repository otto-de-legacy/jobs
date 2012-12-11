
-----------------------
Foundational components
-----------------------

* zdaemon
  Daemon process control library and tools for Unix-based systems
  http://pypi.python.org/pypi/zdaemon

* flask
  Flask is a microframework for Python based on Werkzeug, Jinja 2 and good intentions.
  http://flask.pocoo.org

* fabric
  Fabric is a Python library and command-line tool for streamlining the use of SSH for
  application deployment or systems administration tasks.
  http://fabfile.org


------------
Requirements
------------

* sudo apt-get install python-pip
* sudo pip install --upgrade fabric zdaemon Flask


---------------
Getting started
---------------

export JOBMONITOR_SETTINGS=jobmonitor_settings.cfg

Start local HTTP server with:

    python jobmonitor.py

... should now listen on port 5000

    curl -i http://127.0.0.1:5000/

and return version information.
List available jobs:

    curl -i http://127.0.0.1:5000/jobs/


To start a new job:

    curl -i -X POST -H'Content-Type: application/json' http://127.0.0.1:5000/jobs/demojob -d @samples/demojob.json

To stop a job:

    curl -i -X DELETE http://127.0.0.1:5000/jobs/demojob
