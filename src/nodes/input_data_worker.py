import sys
import json
import argparse
from os import path
from pathlib import Path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from workers.input import InputDataWorker

def main(config):

    with open(config) as f:
        config_data = json.load(f)

    worker = InputDataWorker(config_data)

    worker.run()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description='Input worker NBA Statistics',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            '--config',
            help='The Net Topology configuration file'
    )

    args = parser.parse_args()

    main(args.config)

