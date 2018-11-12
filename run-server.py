#!/usr/bin/python3

import argparse
from subprocess import Popen

#
# Runs all the nodes in background
#  and store all the pids in a file
#  to terminate them after processing
#

PYTHON="python3"
NODES_DIR="src/nodes/"
CONFIG="--config=src/config.json"

def run(summary_workers,
        summary_reducers,
        topk_workers,
        topk_reducers,
        local_team_workers,
        local_points_workers,
        input_workers):

        pids = []

        #########################
        ## Input workers nodes ##
        #########################

        for wid in range(1, input_workers + 1):
            p = Popen([PYTHON,
                       NODES_DIR + "input_data_worker.py",
                       CONFIG])
            pids.append(p.pid)

        ###############################
        ## Match Summary spawn nodes ##
        ###############################

        p = Popen([PYTHON,
                   NODES_DIR + "match_summary_filter.py",
                   "--iworkers={}".format(input_workers),
                   CONFIG])
        pids.append(p.pid)

        for wid in range(1, summary_workers + 1):
            p = Popen([PYTHON,
                       NODES_DIR + "match_summary_worker.py",
                       CONFIG,
                       "--reducers={}".format(summary_reducers)])
            pids.append(p.pid)

        p = Popen([PYTHON,
                   NODES_DIR + "match_summary_proxy.py",
                   CONFIG])
        pids.append(p.pid)

        for rid in range(1, summary_reducers + 1):
            p = Popen([PYTHON,
                       NODES_DIR + "match_summary_reducer.py",
                       "--workers={}".format(summary_workers),
                       "--rid={}".format(rid),
                       CONFIG])
            pids.append(p.pid)

        p = Popen([PYTHON,
                   NODES_DIR + "match_summary.py",
                   "--reducers={}".format(summary_reducers),
                   CONFIG])
        pids.append(p.pid)

        ################################
        ## Run 'Local Team Won' nodes ##
        ################################

        for wid in range(1, local_team_workers + 1):
            p = Popen([PYTHON,
                       NODES_DIR + "local_team_worker.py",
                       CONFIG])
            pids.append(p.pid)

        p = Popen([PYTHON,
                   NODES_DIR + "local_team.py",
                   CONFIG,
                   "--workers={}".format(local_team_workers)])
        pids.append(p.pid)

        ##############################
        ## Run 'Local Points' nodes ##
        ##############################

        p = Popen([PYTHON,
                   NODES_DIR + "local_points_filter.py",
                   "--iworkers={}".format(input_workers),
                   CONFIG])
        pids.append(p.pid)

        for wid in range(1, local_points_workers + 1):
            p = Popen([PYTHON,
                       NODES_DIR + "local_points_worker.py",
                       CONFIG])
            pids.append(p.pid)
        
        p = Popen([PYTHON,
                   NODES_DIR + "local_points.py",
                   CONFIG,
                   "--workers={}".format(local_points_workers)])
        pids.append(p.pid)

        #######################
        ## Run 'Top K' nodes ##
        #######################

        p = Popen([PYTHON,
                   NODES_DIR + "topk_filter.py",
                   "--iworkers={}".format(input_workers),
                   CONFIG])
        pids.append(p.pid)

        for wid in range(1, topk_workers + 1):
            p = Popen([PYTHON,
                       NODES_DIR + "topk_worker.py",
                       CONFIG,
                       "--reducers={}".format(topk_reducers)])
            pids.append(p.pid)

        p = Popen([PYTHON,
                   NODES_DIR + "topk_proxy.py",
                   CONFIG])
        pids.append(p.pid)

        for rid in range(1, topk_reducers + 1):
            p = Popen([PYTHON,
                       NODES_DIR + "topk_reducer.py",
                       "--workers={}".format(topk_workers),
                       "--rid={}".format(rid),
                       CONFIG])
            pids.append(p.pid)

        p = Popen([PYTHON,
                   NODES_DIR + "topk.py",
                   CONFIG,
                   "--reducers={}".format(topk_reducers)])
        pids.append(p.pid)

        return pids

def store(pids):

    with open("pids.store", "w+") as f:
        for pid in pids:
            f.write(str(pid) + "\n")

def main(summary_workers,
         summary_reducers,
         topk_workers,
         topk_reducers,
         local_team_workers,
         local_points_workers,
         input_workers):
    
    pids = run(summary_workers,
               summary_reducers,
               topk_workers,
               topk_reducers,
               local_team_workers,
               local_points_workers,
               input_workers)
    
    store(pids)

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
    parser.add_argument(
            '--iworkers',
            type=int,
            default=2,
            help='Number of Input workers'
    )
    
    args = parser.parse_args()

    main(args.mworkers,
         args.mreducers,
         args.topkworkers,
         args.topkreducers,
         args.ltworkers,
         args.lpworkers,
         args.iworkers)

