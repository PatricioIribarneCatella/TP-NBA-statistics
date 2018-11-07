#!/bin/bash

#############
# Run nodes #
#############

###############################
## Run 'Match Summary' nodes ##
###############################

python3 nodes/match_summary_filter.py --config="config.json" &

for wid in {1..${MATCH_WORKERS}}; do
	python3 nodes/match_summary_worker.py --config="config.json" --reducers=${MATCH_REDUCERS} &
done

python3 nodes/match_summary_proxy.py --config="config.json" &

for id in {1..${MATCH_REDUCERS}}; do
	python3 nodes/match_summary_reducer.py --workers=${MATCH_WORKERS} --rid=$id --config="config.json" &
done

python3 nodes/match_summary.py --reducers=3 --config="config.json" &

################################
## Run 'Local Team Won' nodes ##
################################
for id in {1..2}; do
	python3 nodes/local_team_worker.py --config="config.json" &
done

python3 nodes/local_team.py --config="config.json" --workers=2 &

##############################
## Run 'Local Points' nodes ##
##############################
python3 nodes/local_points_filter.py --config="config.json" &

for id in {1..2}; do
	python3 nodes/local_points_worker.py --config="config.json" &
done

python3 nodes/local_points.py --config="config.json" --workers=2 &

#######################
## Run 'Top K' nodes ##
#######################
python3 nodes/topk_filter.py --config="config.json" &
python3 nodes/topk_worker.py --config="config.json" --reducers=3 &
python3 nodes/topk_proxy.py --config="config.json" &

for id in {1..3}; do
	python3 nodes/topk_reducer.py --workers=1 --rid=$id --config="config.json" &
done

python3 nodes/topk.py --reducers=3 --config="config.json" &

