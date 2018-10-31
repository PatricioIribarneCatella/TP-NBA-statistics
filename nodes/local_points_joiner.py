import sys
import argparse
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from joiners.join import JoinCounter

def main(port):

    joiner = JoinCounter(port)

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

    args = parser.parse_args()

    main(args.port)
