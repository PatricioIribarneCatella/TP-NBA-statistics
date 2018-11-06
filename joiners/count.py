import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from middleware.connection import GatherSocket

#
# format(msg) -> two_ok total_two three_ok total_three_points
#
class LocalPointsCounter(object):

    def __init__(self, workers, config):
        self.workers = workers
        self.socket = GatherSocket(config["local-points"])

    def run(self):
        
        print("Local Points counter started")

        i = 0
        total_two_points = 0
        two_ok = 0
        total_three_points = 0
        three_ok = 0

        while i < self.workers:
           
            msg = self.socket.recv()

            two_scored, total_two, three_scored, total_three = msg.split(" ")

            two_ok += int(two_scored)
            total_two_points += int(total_two)
            three_ok += int(three_scored)
            total_three_points += int(total_three)

            i += 1

        2_points = round(two_ok/total_two_points, 4) * 100
        3_points = round(three_ok/total_three_points, 4) * 100 

        print("2 pts: {}%, 3 pts: {}%".format(2_points, 3_points))

        print("Local Points counter finished")

#
# format(msg) -> total_home_team total_matches
#
class LocalTeamCounter(object):

    def __init__(self, workers, config):
        self.workers = workers
        self.socket = GatherSocket(config["local-team"])

    def run(self):

        print("Local Team counter started")

        i = 0
        total_matches = 0
        total_home_team = 0

        while i < self.workers:

            msg = self.socket.recv()

            home_team, matches = msg.split(" ")

            total_matches += int(matches)
            total_home_team += int(home_team)

            i += 1

        local_team = round(total_home_team/total_matches, 4) * 100

        print("local team: {}%".format(local_team))

        print("Local Team counter finished")


