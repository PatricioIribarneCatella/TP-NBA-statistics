import sys
import argparse
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from reducers.match_summary import MatchSummaryReducer

def main(rid, reduce_port, join_port, num_workers):

    reducer = MatchSummaryReducer(rid, reduce_port, join_port, num_workers)

    reducer.run()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description='Match summary reducer NBA',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            '--rport',
            type=int,
            default=6666,
            help='The port to receive data from workers'
    )
    parser.add_argument(
            '--jport',
            type=int,
            default=8888,
            help='The port to send reduced data'
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
    args = parser.parse_args()

    main(args.rid, args.rport, args.jport, args.workers)

