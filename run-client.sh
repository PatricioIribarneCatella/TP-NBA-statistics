# Runs the client with the input data

python3 nodes/main.py --data="NBA-data/shot log*" --config="config.json" --pattern="[player=shoot player,shot_result=current shot outcome,home_scored=home game,home_team=home team,away_team=away team,points=points,date=date]"
