import sys
import argparse
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from workers.matches_summary import MatchSummaryWorker

def main(wport, jport, reducers):

    worker = MatchSummaryWorker(wport, jport, reducers)

    worker.run()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description='Matches summary worker NBA',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            '--port',
            type=int,
            default=6666,
            help='The port to receive data from dispatcher'
    )
    parser.add_argument(
            '--jport',
            type=int,
            default=8888,
            help='The port to send processed data'
    )
    parser.add_argument(
            '--reducers',
            type=int,
            default=1,
            help='How many reducers are working'
    )
    args = parser.parse_args()

    main(args.port, args.jport, args.reducers)

