#!/usr/bin/python3

import argparse
from subprocess import Popen

#
# Runs the client node
#

PYTHON="python3"
NODE_DIR="src/nodes/"
DATA="data/shot log*"
PATTERN="[player=shoot player,shot_result=current shot outcome,home_scored=home game]"
CONFIG_FILE="src/config.json"

def main(nstats):

    p = Popen([PYTHON,
               "{}main.py".format(NODE_DIR),
               "--data={}".format(DATA),
               "--nstats={}".format(nstats),
               "--pattern={}".format(PATTERN),
               "--config={}".format(CONFIG_FILE)])
    p.wait()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description='NBA Statistics Script Generator',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            '--nstats',
            type=int,
            default=4,
            help='Number of Statistics to perform'
    )

    args = parser.parse_args()

    main(args.nstats)

