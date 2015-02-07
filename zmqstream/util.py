# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

import sys
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
            sys.stdout.write(" %s  %s\r" % (self.next(), self.message.ljust(35)))
            sys.stdout.flush()




def main():

    activity_indicator = ActivityIndicator(ActivityIndicator.LINE_FRAMES)
    while True:
        activity_indicator.tick()


if __name__ == "__main__":
    main()