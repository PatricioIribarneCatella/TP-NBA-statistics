import sys
import json
import argparse
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from middleware.proxy import TopkProxy

def main(config):

    with open(config) as f:
        config_data = json.load(f)

    proxy = TopkProxy(config_data)

    proxy.run()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description='Top K proxy NBA',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            '--config',
            help='The Net Topology configuration file'
    )
    
    args = parser.parse_args()

    main(args.config)

