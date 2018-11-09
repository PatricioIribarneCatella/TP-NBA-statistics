import sys
import json
import argparse
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from joiners.count import LocalPointsCounter

def main(workers, config):

    with open(config) as f:
        config_data = json.load(f)

    joiner = LocalPointsCounter(workers, config_data)

    joiner.run()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description='Local points NBA',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            '--config',
            help='The Net Topology configuration file'
    )
    parser.add_argument(
            '--workers',
            type=int,
            default=1,
            help='The quantity of workers connected to it'
    )

    args = parser.parse_args()

    main(args.workers, args.config)


