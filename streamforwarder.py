# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals







def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('arg1', type=str, help="required argument")
    parser.add_argument('arg2', '--argument-2', help='optional boolean argument', action="store_true")
    args = parser.parse_args()


if __name__ == "__main__":
    main()