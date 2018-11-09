import sys
import json
import argparse
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from coordinators.filters import MatchSummaryFilter

def main(config):

    with open(config) as f:
        config_data = json.load(f)

    replicator = MatchSummaryFilter(config_data)

    replicator.run()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description='Match summary Filter NBA',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            '--config',
            help='The Net Topology configuration file'
    )

    args = parser.parse_args()

    main(args.config)

