#!/bin/bash

# Virtualenv setup and PyZmq installation
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt

# Run all the Nodes
python3 nodes/filter.py --pattern="shot_result=SCORED" --config="config.json" &
python3 nodes/match_summary_worker.py --reducers=3 --config="config.json" &
python3 nodes/worker_reducer_proxy.py --config="config.json" &

for id in {1..3}; do
	python3 nodes/match_summary_reducer.py --workers=1 --rid=$id --config="config.json" &
done

python3 nodes/match_summary.py --reducers=3 --config="config.json" &

