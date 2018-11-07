import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from middleware.connection import GatherSocket, ProducerSocket
import middleware.constants as const

#
# format(msg) -> two_ok total_two three_ok total_three_points
#
class LocalPointsCounter(object):

    def __init__(self, workers, config):
        self.workers = workers
        self.socket = GatherSocket(config["local-points"])
        self.stats_socket = ProducerSocket(config["local-points"]["stats"])

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

        two_pts = round(two_ok/total_two_points, 4) * 100
        three_pts = round(three_ok/total_three_points, 4) * 100 

        self.stats_socket.send("{} 2 pts: {}%, 3 pts: {}%".format(
                const.LOCAL_POINTS_STAT, two_pts, three_pts))

        print("2 pts: {}%, 3 pts: {}%".format(two_pts, three_pts))
        print("Local Points counter finished")

#
# format(msg) -> total_home_team total_matches
#
class LocalTeamCounter(object):

    def __init__(self, workers, config):
        self.workers = workers
        self.socket = GatherSocket(config["local-team"])
        self.stats_socket = ProducerSocket(config["local-team"]["stats"])

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

        self.stats_socket.send("{} local team: {}%".format(
            const.LOCAL_TEAM_STAT, local_team))
        
        print("local team: {}%".format(local_team))
        print("Local Team counter finished")


