import os, binascii

# create random ascii string for identifying job
rnd = binascii.b2a_hex(os.urandom(6))

#rv = self.app.post('/jobs/demojob', content_type='application/json', data=json.dumps(payload))
#resp_js = flask.json.loads(rv.data)
