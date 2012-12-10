import os, binascii

# create random ascii string for identifying job
rnd = binascii.b2a_hex(os.urandom(6))
