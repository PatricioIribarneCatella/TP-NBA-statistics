import sys
import argparse
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from joiners.join import JoinCounter

def main(port, workers):

    joiner = JoinCounter(port, workers)

    joiner.run()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description='Scored Filter NBA',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            '--port',
            type=int,
            default=8888,
            help='The port to connect to local points workers'
    )
    parser.add_argument(
            '--workers',
            type=int,
            default=1,
            help='The quantity of workers connected to it'
    )

    args = parser.parse_args()

    main(args.port, args.workers)
