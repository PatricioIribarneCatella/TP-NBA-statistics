import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from middleware.connection import GatherSocket, ProducerSocket

import middleware.constants as const

class Topk(object):

    def __init__(self, reducers, k_number, config):

        self.socket = GatherSocket(config["topk"])
        self.stats_socket = ProducerSocket(config["topk"]["stats"])
        self.signal_proxy_socket = ProducerSocket(config["topk"]["signal-proxy"])
 
        self.topk_number = k_number
        self.num_reducers = reducers
        self.data = {}

    def _process_data(self, msg):

        player, points = msg.split("\n")

        self.data[player] = points

    def _calculate_topk(self):
        
        items = list(self.data.items())

        s = sorted(items, key=lambda it: it[1], reverse=True)

        s = s[:self.topk_number]

        for player_info in s:
            self.stats_socket.send("{} {} {}".format(
                    const.TOPK_STAT, player_info[0], player_info[1]))
            print("{}, {}".format(player_info[0], player_info[1]))

        self.stats_socket.send("{} {}".format(
            const.END_DATA_ID, const.END_DATA))

    def run(self):

        print("Top K started")

        end_data_counter = 0

        while end_data_counter < self.num_reducers:

            msg = self.socket.recv()

            if msg == const.END_DATA:
                end_data_counter += 1
            else:
                self._process_data(msg)

        self._calculate_topk()

        # Send signal to proxy
        self.signal_proxy_socket.send(const.END_DATA)

        self.socket.close()
        self.signal_proxy_socket.close()
        
        print("Top K finished")


