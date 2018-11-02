import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from middleware.connection import GatherSocket

#
# format(msg) -> home_team home_points away_points away_team\n
#
class MatchSummary(object):

    def __init__(self, port, reducers):

        self.num_reducers = reducers
        self.socket = GatherSocket(port)

    def run(self):

        print("Match summary started")

        end_data_counter = 0

        while end_data_counter < self.num_reducers:

            msg = self.socket.recv()

            if msg == "END_DATA":
                end_data_counter += 1
            else:
                print(msg)

        self.socket.close()
        print("Match summary finished")
