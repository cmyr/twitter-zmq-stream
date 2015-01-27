# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

import zmq

def zmq_iter(port):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.setsockopt_string(zmq.SUBSCRIBE, '')
    socket.connect("tcp://localhost:%s" % port)
    while True:
        try:
            yield socket.recv_string()
        except KeyboardInterrupt as err:
            socket.close()
            context.term()
            break

def main():
    for msg in zmq_iter("8069"):
        print(msg)

if __name__ == "__main__":
    main()

# # def main():
# #     import argparse
# #     parser = argparse.ArgumentParser()
# #     parser.add_argument('arg1', type=str, help="required argument")
# #     parser.add_argument('arg2', '--argument-2', help='optional boolean argument', action="store_true")
# #     args = parser.parse_args()


# # if __name__ == "__main__":
# #     main()