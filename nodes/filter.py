import sys
import json
import argparse
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from coordinators.filter_dispatcher import DataFilterReplicator

def main(pattern, config):

    with open(config) as f:
        config_data = json.load(f)

    replicator = DataFilterReplicator(pattern, config_data)

    replicator.run()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description='Scored Filter NBA',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            '--config',
            help='The Net Topology configuration file'
    )
    parser.add_argument(
            '--pattern',
            help='The pattern to filter in'
    )
    args = parser.parse_args()

    main(args.pattern, args.config)
