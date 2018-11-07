import sys
import json
import argparse
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from joiners.summary import MatchSummary

def main(num_reducers, config):

    with open(config) as f:
        config_data = json.load(f)

    reducer = MatchSummary(num_reducers, config_data)

    reducer.run()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description='Match summary NBA',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            '--config',
            help='The Net Topology config file'
    )
    parser.add_argument(
            '--reducers',
            type=int,
            default=1,
            help='The number of summary reducers'
    )
    args = parser.parse_args()

    main(args.reducers, args.config)

