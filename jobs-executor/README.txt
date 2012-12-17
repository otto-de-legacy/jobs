
-----------------------
Foundational components
-----------------------

* zdaemon
  Daemon process control library and tools for Unix-based systems
  http://pypi.python.org/pypi/zdaemon

* fabric
  Fabric is a Python library and command-line tool for streamlining the use of SSH for
  application deployment or systems administration tasks.
  http://fabfile.org

* flask
  Flask is a microframework for Python based on Werkzeug, Jinja 2 and good intentions.
  http://flask.pocoo.org

------------
Requirements
------------

* on the local system running the jobmonitor
    sudo apt-get install python-pip
    sudo pip install --upgrade fabric Flask

* on the remote system for job execution (by default: localhost):
    sudo apt-get install python-zdaemon



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

To upload a new job:

    curl -i -X POST http://127.0.0.1:5000/jobs/test_job -d @samples/demojob.json


To start a new job instance:

    curl -i -X POST -H'Content-Type: application/json' http://127.0.0.1:5000/jobs/demojob/start -d @samples/demojob.json

As part of the response you will get inside the response header a link
which you can follow to get more information about the job instance:

    curl -i http://localhost:5000/jobs/demojob/030bb50f4571

To stop this job instance simply run:

    curl -i -X DELETE http://127.0.0.1:5000/jobs/demojob/030bb50f4571


--------------
For developers
--------------

Execute test suite:

    python jobmonitor_tests.py

Run single test:

    python jobmonitor_tests.py JobMonitorIntegrationTests.test_get_job_instance

Run all unit tests:

    python jobmonitor_tests.py JobMonitorUnitTests

Execute tests and measuring coverage:
(requires: pip install nosexcover)

    nosetests --with-xcoverage --with-xunit --cover-package=jobmonitor

Run lint over sources:
(requires: pip install pylint)

    pylint -f parseable jobmonitor.py