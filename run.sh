virtualenv venv
source venv/bin/activate
pip install -r requirements.txt

python3 nodes/filter.py --pattern="shot_result=SCORED" &
python3 nodes/match_summary_worker.py --reducers=3 &
python3 nodes/worker_reducer_proxy.py --wport=8888 --rport=9999 &
python3 nodes/match_summary_reducer.py --rport=9999 --jport=10000 --rid=1 &
python3 nodes/match_summary_reducer.py --rport=9999 --jport=10000 --rid=2 &
python3 nodes/match_summary_reducer.py --rport=9999 --jport=10000 --rid=3 &
python3 nodes/match_summary.py --jport=10000 --reducers=3 &

