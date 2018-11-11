import sys
import json
import argparse
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from filters.summary import MatchSummaryFilter

def main(config, input_workers):

    with open(config) as f:
        config_data = json.load(f)

    replicator = MatchSummaryFilter(input_workers, config_data)

    replicator.run()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description='Match summary Filter NBA',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            '--config',
            help='The Net Topology configuration file'
    )
    parser.add_argument(
            '--iworkers',
            type=int,
            default=2,
            help='The number of input workers'
    )

    args = parser.parse_args()

    main(args.config, args.iworkers)

