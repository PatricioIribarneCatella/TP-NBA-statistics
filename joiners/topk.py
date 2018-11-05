import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from middleware.connection import GatherSocket

class Topk(object):

    def __init__(self, reducers, config):

        self.num_reducers = reducers
        self.socket = GatherSocket(config["topk"])
        self.data = {}
        self.topk_number = 10

    def _process_data(self, msg):

        player, points = msg.split("\n")

        self.data[player] = points

    def _calculate_topk(self):
        
        items = list(self.data.items())

        s = sorted(items, key=lambda it: it[1], reverse=True)

        s = s[:self.topk_number]

        for player_info in s:
            print("{}, {}".format(player_info[0], player_info[1]))

    def run(self):

        print("Top K started")

        end_data_counter = 0

        while end_data_counter < self.num_reducers:

            msg = self.socket.recv()

            if msg == "END_DATA":
                end_data_counter += 1
            else:
                self._process_data(msg)

        self.socket.close()

        self._calculate_topk()

        print("Top K finished")


