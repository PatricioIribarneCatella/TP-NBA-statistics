#!/usr/bin/python3

import argparse

#
# Automatically generates a bash script
#

def generate(summary_workers,
             summary_reducers,
             topk_workers,
             topk_reducers,
             local_team_workers,
             local_points_workers):

    print("#!/bin/bash")

    print("#############")
    print("# Run nodes #")
    print("#############")

    print("###############################")
    print("## Run 'Match Summary' nodes ##")
    print("###############################")

    print("python3 nodes/match_summary_filter.py --config=config.json &")

    print("for wid in {1.." + str(summary_workers) + "}; do")
    print("     python3 nodes/match_summary_worker.py --config=config.json --reducers={} &".format(summary_reducers))
    print("done")

    print("python3 nodes/match_summary_proxy.py --config=config.json &")

    print("for id in {1.." + str(summary_reducers) + "}; do")
    print("     python3 nodes/match_summary_reducer.py --workers={} --rid=$id --config=config.json &".format(summary_workers))
    print("done")

    print("python3 nodes/match_summary.py --reducers={} --config=config.json &".format(summary_reducers))

    print("################################")
    print("## Run 'Local Team Won' nodes ##")
    print("################################")

    print("for id in {1.." + str(local_team_workers) + "}; do")
    print("     python3 nodes/local_team_worker.py --config=config.json &")
    print("done")

    print("python3 nodes/local_team.py --config=config.json --workers={} &".format(local_team_workers))

    print("##############################")
    print("## Run 'Local Points' nodes ##")
    print("##############################")

    print("python3 nodes/local_points_filter.py --config=config.json &")

    print("for id in {1.." + str(local_points_workers) + "}; do")
    print("     python3 nodes/local_points_worker.py --config=config.json &")
    print("done")

    print("python3 nodes/local_points.py --config=config.json --workers={} &".format(local_points_workers))

    print("#######################")
    print("## Run 'Top K' nodes ##")
    print("#######################")

    print("python3 nodes/topk_filter.py --config=config.json &")
    
    print("for wid in {1.." + str(topk_workers) + "}; do")
    print("     python3 nodes/topk_worker.py --config=config.json --reducers={} &".format(topk_reducers))
    print("done")

    print("python3 nodes/topk_proxy.py --config=config.json &")

    print("for id in {1.." + str(topk_reducers) + "}; do")
    print("     python3 nodes/topk_reducer.py --workers={} --rid=$id --config=config.json &".format(topk_workers))
    print("done")

    print("python3 nodes/topk.py --reducers={} --config=config.json &".format(topk_reducers))


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
                    description='NBA Statistics Script Generator',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            '--mworkers',
            type=int,
            default=1,
            help='Number of Match summary workers'
    )
    parser.add_argument(
            '--mreducers',
            type=int,
            default=2,
            help='Number of Match Summary reducers'
    )
    parser.add_argument(
            '--topkworkers',
            type=int,
            default=1,
            help='Number of Top K workers'
    )
    parser.add_argument(
            '--topkreducers',
            type=int,
            default=2,
            help='Number of Top K reducers'
    )
    parser.add_argument(
            '--ltworkers',
            type=int,
            default=2,
            help='Number of Local Team workers'
    )
    parser.add_argument(
            '--lpworkers',
            type=int,
            default=2,
            help='Number of Local Points workers'
    )
    
    args = parser.parse_args()

    generate(args.mworkers,
             args.mreducers,
             args.topkworkers,
             args.topkreducers,
             args.ltworkers,
             args.lpworkers)

