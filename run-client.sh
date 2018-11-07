#!/bin/bash

# Runs the client with the input data

STATS=$1
NSTATS="${STATS#*=}"

DATA="NBA-data/shot log*"
CONFIG="config.json"
PATTERN="[player=shoot player,shot_result=current shot outcome,home_scored=home game,home_team=home team,away_team=away team,points=points,date=date]"

python3 nodes/main.py --data="${DATA}" --config="${CONFIG}" --nstats="${NSTATS}" --pattern="${PATTERN}"

