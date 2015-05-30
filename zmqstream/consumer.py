# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

import zmq
import zmq.ssh



def zmq_iter(host="localhost", port=8069, tunnel=None):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.setsockopt_string(zmq.SUBSCRIBE, '')
    socket_address = "tcp://%s:%s" % (host, str(port))
    if tunnel:
        zmq.ssh.tunnel_connection(socket, socket_address, tunnel)
    else:
        socket.connect(socket_address)
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
    parser.add_argument('-t', '--tunnel', type=str, help="sever for tunneling over ssh")
    args = parser.parse_args()

    funcargs = dict()
    if args.hostname:
        funcargs['host'] = args.hostname
    if args.port:
        funcargs['port'] = args.port
    if args.tunnel:
        funcargs['tunnel'] = args.tunnel

    for msg in zmq_iter(**funcargs):
        if args.raw:
            print(json.dumps(msg))
        else:
            print(msg.get('text').encode('utf-8'))


if __name__ == "__main__":
    main()