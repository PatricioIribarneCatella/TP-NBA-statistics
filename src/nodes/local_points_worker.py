import sys
import json
import argparse
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from workers.local_points import LocalPointsWorker

def main(config):

    with open(config) as f:
        config_data = json.load(f)
        
    worker = LocalPointsWorker(config_data)

    worker.run()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description='Local points worker NBA',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            '--config',
            help='The Net Topology configuration file'
    )

    args = parser.parse_args()

    main(args.config)

