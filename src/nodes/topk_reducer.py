import sys
import json
import argparse
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from reducers.topk import TopkReducer

def main(rid, num_workers, k_number, config):

    with open(config) as f:
        config_data = json.load(f)

    reducer = TopkReducer(rid, num_workers, k_number, config_data)

    reducer.run()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description='Top K reducer NBA',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            '--config',
            help='The Net Topology configuration file'
    )
    parser.add_argument(
            '--rid',
            type=int,
            default=1,
            help="The Reducer id"
    )
    parser.add_argument(
            '--workers',
            type=int,
            default=1,
            help='The number of summary workers'
    )
    parser.add_argument(
            '--k',
            type=int,
            default=10,
            help='The k of the Top K'
    )

    args = parser.parse_args()

    main(args.rid, args.workers, args.k, args.config)

