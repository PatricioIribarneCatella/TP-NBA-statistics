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
        
        print("Join counter started")

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

        print("Join counter finished")

class LocalTeamCounter(object):

    def __init__(self, workers, config):
        self.workers = workers
        self.socket = GatherSocket(config["local-team"])

    def run(self):


