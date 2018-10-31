import sys
import argparse
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from coordinators.filter_replicator import DataFilterReplicator

def main(port):

    replicator = DataFilterReplicator(port)

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
    args = parser.parse_args()

    main(args.port)
