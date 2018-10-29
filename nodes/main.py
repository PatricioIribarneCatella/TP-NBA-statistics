import sys
import argparse
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from coordinators.replicator import DataReplicator

def main(data, pattern):

    replicator = DataReplicator(data, pattern)

    replicator.run()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description='NBA Statistics',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            '--data',
            help='Origin data'
    )
    parser.add_argument(
            '--pattern',
            help='Match pattern for data fields'
    )
    args = parser.parse_args()

    main(args.data, args.pattern)
