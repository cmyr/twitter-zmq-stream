# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

import sys
import os
import time

class ActivityIndicator(object):
    BULLET_FRAMES = ["■","◆","✦","★","✶","✷","✹","✺","◉","◎","◦","●","∙"]
    LINE_FRAMES = ["_","⎽","⎼","⎻","⎺","⎻","⎼","⎽"]
    TRI_FRAMES = ["ᗑ","ᗒ","ᗐ","ᗕ","ᗅ","ᗆ","ᗄ","ᗉ","ᗋ","ᗌ","ᗊ","ᗏ","ᐃ","ᐅ","ᐁ","ᐊ","ᐄ","ᐓ","ᐍ","ᐗ"]
    OLD_FRAMES = ["_", ",", ".", "•","*", "°", "ˆ", "´", "`", "¨"]

    """docstring for ActivityIndicator"""
    def __init__(self, frames=None, min_interval=0.1, message=""):
        super(ActivityIndicator, self).__init__()
        self.min_interval = min_interval
        self.message = message
        self.indicatorFrames = frames or ActivityIndicator.BULLET_FRAMES
        self.index = 0
        self.last_tick = time.time()


    def __str__(self):
        return self.next()

    def next(self):
        result = self.indicatorFrames[self.index]
        self.index = (self.index + 1) % len(self.indicatorFrames)
        return result

    def tick(self):
        if time.time() - self.last_tick > self.min_interval:
            self.last_tick = time.time()
            message = " %s  %s\r" % (self.next(), self.message.ljust(35))
            sys.stdout.write(message.encode('utf-8'))
            sys.stdout.flush()


def keys_dirs():
    """ returns paths to the public and secret key directories"""
    keys_dir = os.path.expanduser('~/.zmqauth')
    public_keys_dir = os.path.join(keys_dir, 'public_keys')
    secret_keys_dir = os.path.join(keys_dir, 'private_keys')
    if not (os.path.exists(keys_dir) and
            os.path.exists(public_keys_dir) and
            os.path.exists(secret_keys_dir)):
        print(
            "Certificates are missing, will exit")
        sys.exit(1)
    return (public_keys_dir, secret_keys_dir)


def main():

    activity_indicator = ActivityIndicator(ActivityIndicator.LINE_FRAMES)
    while True:
        activity_indicator.tick()


if __name__ == "__main__":
    main()