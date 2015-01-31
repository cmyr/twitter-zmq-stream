# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

import sys

class ActivityIndicator(object):
    BULLET_FRAMES = ["■","◆","✦","★","✶","✷","✹","✺","◉","◎","◦","●","∙"]
    LINE_FRAMES = ["_","⎽","⎼","⎻","⎺","⎻","⎼","⎽"]

    """docstring for ActivityIndicator"""
    def __init__(self, frames=None):
        super(ActivityIndicator, self).__init__()
        self.indicatorFrames = frames or ["_", ",", ".", "•","*", "°", "ˆ", "´", "`", "¨"]
        self.index = 0

    def __str__(self):
        return self.next()

    def next(self):
        result = self.indicatorFrames[self.index]
        self.index = (self.index + 1) % len(self.indicatorFrames)
        return result

    def tick(self):
        sys.stdout.write(" %s\r" % self.next())
        sys.stdout.flush()




def main():
    import time

    activity_indicator = ActivityIndicator(ActivityIndicator.LINE_FRAMES)
    while True:
        activity_indicator.tick()
        time.sleep(0.2)


if __name__ == "__main__":
    main()