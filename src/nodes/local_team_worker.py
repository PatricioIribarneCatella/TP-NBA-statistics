import sys
import json
import argparse
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from src.workers.local_team import LocalTeamWorker

def main(config):

    with open(config) as f:
        config_data = json.load(f)
        
    worker = LocalTeamWorker(config_data)

    worker.run()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description='Local team worker NBA',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            '--config',
            help='The Net Topology configuration file'
    )

    args = parser.parse_args()

    main(args.config)

