# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

import zmq
import zmq.auth
import os
try:
    from .util import keys_dirs
except:
    from util import keys_dirs

def zmq_iter(host="localhost", port=8069, require_auth=False):
    if require_auth:
        public_keys_dir, secret_keys_dir = keys_dirs()
        if not (os.path.exists(keys_dir) and
                os.path.exists(public_keys_dir) and
                os.path.exists(secret_keys_dir)):
                    print("Certificates are missing - run generate_certificates.py script first")
                    sys.exit(1)

    
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    if require_auth:
        client_secret_file = os.path.join(secret_keys_dir, "client.key_secret")
        client_public, client_secret = zmq.auth.load_certificate(client_secret_file)
        socket.curve_secretkey = client_secret
        socket.curve_publickey = client_public

        server_public_file = os.path.join(public_keys_dir, "server.key")
        server_public, _ = zmq.auth.load_certificate(server_public_file)
        # The client must know the server's public key to make a CURVE connection.
        socket.curve_serverkey = server_public

    socket.setsockopt_string(zmq.SUBSCRIBE, '')
    socket.connect("tcp://%s:%s" % (host, str(port)))
    while True:
        try:
            result = socket.recv_json()
            if isinstance(result, dict):
                yield result
        except KeyboardInterrupt as err:
            socket.close()
            context.term()
            break


def main():
    import argparse
    import json

    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--hostname', type=str, default="localhost",
                        help="publisher hostname")
    parser.add_argument('-p', '--port', type=str, help="publisher port")
    parser.add_argument('-r', '--raw',
                        action="store_true", help="output raw json")
    args = parser.parse_args()

    funcargs = dict()
    if args.hostname:
        funcargs['host'] = args.hostname
    if args.port:
        funcargs['port'] = args.port

    for msg in zmq_iter(**funcargs):
        if args.raw:
            print(json.dumps(msg))
        else:
            print(msg.get('text').encode('utf-8'))


if __name__ == "__main__":
    main()