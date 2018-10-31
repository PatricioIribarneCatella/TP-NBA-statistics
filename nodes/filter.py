import sys
import argparse
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from coordinators.filter_replicator import DataFilterReplicator

def main(port, dport, pattern):

    replicator = DataFilterReplicator(port, dport, pattern)

    replicator.run()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description='Scored Filter NBA',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            '--port',
            type=int,
            default=5555,
            help='The port to connect to replicator server'
    )
    parser.add_argument(
            '--dport',
            type=int,
            default=6666,
            help='The dispatcher port'
    )
    parser.add_argument(
            '--pattern',
            help='The pattern to filter in'
    )
    args = parser.parse_args()

    main(args.port, args.dport, args.pattern)
