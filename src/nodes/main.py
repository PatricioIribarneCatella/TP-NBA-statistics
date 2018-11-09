import sys
import json
import argparse
from os import path
from pathlib import Path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from src.coordinators.replicator import DataReplicator

def main(data, stats, pattern, num_of_stats, config):

    with open(config) as f:
        config_data = json.load(f)

    # Create stats directory output
    p = Path(stats)

    if not p.exists():
        p.mkdir()

    replicator = DataReplicator(data, stats, pattern, num_of_stats, config_data)

    replicator.run()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description='NBA Statistics',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            '--data',
            help='Origin data'
    )
    parser.add_argument(
            '--stats',
            default='stats/',
            help='Directory to save statistics'
    )
    parser.add_argument(
            '--nstats',
            type=int,
            default=1,
            help='Number of statistics to be performed'
    )
    parser.add_argument(
            '--pattern',
            help='Match pattern for data fields'
    )
    parser.add_argument(
            '--config',
            help='The Net Topology configuration file'
    )

    args = parser.parse_args()

    main(args.data, args.stats, args.pattern, args.nstats, args.config)

