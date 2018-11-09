import sys
import json
import argparse
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from joiners.topk import Topk

def main(num_reducers, k_number, config):

    with open(config) as f:
        config_data = json.load(f)

    reducer = Topk(num_reducers, k_number, config_data)

    reducer.run()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description='Top K NBA',
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
    parser.add_argument(
            '--k',
            type=int,
            default=10,
            help='The k of the Top K'
    )

    args = parser.parse_args()

    main(args.reducers, args.k, args.config)

