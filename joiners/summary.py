import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from middleware.connection import GatherSocket, DispatcherSocket

#
# format(msg) -> home_team home_points away_points away_team
#
class MatchSummary(object):

    def __init__(self, reducers, config):

        self.num_reducers = reducers
        self.socket = GatherSocket(config["match-summary"]["in"])
        self.dispatch_socket = DispatcherSocket(config["match-summary"]["out"])
        self.signal_socket = ReplicationSocket(config["match-summary"]["signal"])

    def run(self):

        print("Match summary started")

        end_data_counter = 0

        while end_data_counter < self.num_reducers:

            msg = self.socket.recv()

            if msg == "END_DATA":
                end_data_counter += 1
            else:
                self.dispatch_socket.send(msg)
                print(msg)

        # Send signal to all the workers
        send.signal_socket.send("0 END_DATA")

        self.socket.close()
        self.dispatch_socket.close()
        self.signal_socket.close()

        print("Match summary finished")


