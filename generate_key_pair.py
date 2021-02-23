#!/opt/python/run/venv/bin/python

"""
Creates an RSA key pair and writes it to redis. Must be run before
every deployment just to ensure there's always a key pair present
and also refresh old one along. 'REDIS_DB_START' environment variable
must be present.
"""

from Cryptodome.PublicKey import RSA
import redis
import os
import subprocess
import sys

if not os.geteuid() == 0:
    print("This script must be run with root privileges.")
    sys.exit(1)

try:
    redis_db_start = subprocess.check_output(['/opt/elasticbeanstalk/bin/get-config',
                                              'environment', '-k', 'REDIS_DB_START'])

    if not redis_db_start:
        redis_db_start = 0

    redis_db_start = int(redis_db_start)

    conn = redis.Redis(os.getenv('REDIS_URI', 'localhost'), db=redis_db_start + 1)

    key = RSA.generate(2048)
    private_key = key.export_key()
    public_key = key.publickey().export_key()

    conn.flushdb()  # remove existing socket sid records as well
    conn.set('pvt_key', private_key)
    conn.set('pub_key', public_key)

    # Check if private key created successfully
    private_key = conn.get('pvt_key')
    if private_key is None:
        print("Weird...Private key not created.")
        sys.exit(1)
except ConnectionError:
    print("Unable to connect to redis")
    sys.exit(1)
