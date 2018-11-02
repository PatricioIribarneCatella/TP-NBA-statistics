import sys
import argparse
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from joiners.summary import MatchSummary

def main(receive_port, num_reducers):

    reducer = MatchSummary(receive_port, num_reducers)

    reducer.run()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description='Match summary NBA',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            '--jport',
            type=int,
            default=8888,
            help='The port to receive reduced data'
    )
    parser.add_argument(
            '--reducers',
            type=int,
            default=1,
            help='The number of summary reducers'
    )
    args = parser.parse_args()

    main(args.jport, args.reducers)

