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

    with open("run-server.sh", "w+") as f:
        
        f.write("#!/bin/bash\n\n")

        f.write("#############\n")
        f.write("# Run nodes #\n")
        f.write("#############\n")

        f.write("\n")

        f.write("###############################\n")
        f.write("## Run 'Match Summary' nodes ##\n")
        f.write("###############################\n")

        f.write("\n")

        f.write("python3 nodes/match_summary_filter.py --config=config.json &\n")

        f.write("for wid in {1.." + str(summary_workers) + "}; do\n")
        f.write("     python3 nodes/match_summary_worker.py --config=config.json --reducers={} &\n".format(summary_reducers))
        f.write("done\n")

        f.write("python3 nodes/match_summary_proxy.py --config=config.json &\n")

        f.write("for id in {1.." + str(summary_reducers) + "}; do\n")
        f.write("     python3 nodes/match_summary_reducer.py --workers={} --rid=$id --config=config.json &\n".format(summary_workers))
        f.write("done\n")

        f.write("python3 nodes/match_summary.py --reducers={} --config=config.json &\n".format(summary_reducers))

        f.write("\n")

        f.write("################################\n")
        f.write("## Run 'Local Team Won' nodes ##\n")
        f.write("################################\n")

        f.write("\n")

        f.write("for id in {1.." + str(local_team_workers) + "}; do\n")
        f.write("     python3 nodes/local_team_worker.py --config=config.json &\n")
        f.write("done\n")

        f.write("python3 nodes/local_team.py --config=config.json --workers={} &\n".format(local_team_workers))

        f.write("\n")

        f.write("##############################\n")
        f.write("## Run 'Local Points' nodes ##\n")
        f.write("##############################\n")

        f.write("\n")

        f.write("python3 nodes/local_points_filter.py --config=config.json &\n")

        f.write("for id in {1.." + str(local_points_workers) + "}; do\n")
        f.write("     python3 nodes/local_points_worker.py --config=config.json &\n")
        f.write("done\n")

        f.write("python3 nodes/local_points.py --config=config.json --workers={} &\n".format(local_points_workers))

        f.write("\n")

        f.write("#######################\n")
        f.write("## Run 'Top K' nodes ##\n")
        f.write("#######################\n")

        f.write("\n")

        f.write("python3 nodes/topk_filter.py --config=config.json &\n")
        
        f.write("for wid in {1.." + str(topk_workers) + "}; do\n")
        f.write("     python3 nodes/topk_worker.py --config=config.json --reducers={} &\n".format(topk_reducers))
        f.write("done\n")

        f.write("python3 nodes/topk_proxy.py --config=config.json &\n")

        f.write("for id in {1.." + str(topk_reducers) + "}; do\n")
        f.write("     python3 nodes/topk_reducer.py --workers={} --rid=$id --config=config.json &\n".format(topk_workers))
        f.write("done\n")

        f.write("python3 nodes/topk.py --reducers={} --config=config.json &\n".format(topk_reducers))

        f.write("\n")

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

