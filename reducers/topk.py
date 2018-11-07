import sys
from os import path


sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from middleware.connection import SuscriberSocket, ProducerSocket

class TopkReducer(object):

    def __init__(self, rid, workers, k_number, config):

        reducer_to_proxy = config["reducer-topk"]["nodes"]["proxy"]
        reducer_to_joiner = config["reducer-topk"]["nodes"]["joiner"]

        self.reduce_socket = SuscriberSocket(reducer_to_proxy, [rid])
        self.joiner_socket = ProducerSocket(reducer_to_joiner)

        self.num_workers = workers

        # It stores (key, value) like this:
        # - key="player"(str)
        # - value=points(int)
        self.data = {}
        self.topk_number = k_number

    def _recv_data(self):

        msg = self.reduce_socket.recv()

        msgid, data = msg.split(" ", 1)

        if (data == "END_DATA"):
            return msgid, data

        data = data.split("\n")

        # Take out the last item
        # that it´s an empty string
        data.pop()

        player = data[0].split("=")[1]
        points = int(data[1].split("=")[1])

        return player, points

    def _process_data(self, key, data):

        if key in self.data:
            self.data[key] += data
        else:
            self.data[key] = data

    def _calculate_topk(self):

        items = list(self.data.items())

        s = sorted(items, key=lambda it: it[1], reverse=True)

        return s[:self.topk_number]

    def _send_data(self):
        
        data = self._calculate_topk()

        for info_player in data:
    
            name = info_player[0]
            points = info_player[1]

            msg = "{}\n{}".format(name, points)

            self.joiner_socket.send(msg)

        # Send 'end' message
        self.joiner_socket.send("END_DATA")

    def run(self):

        print("Top K reducer started")

        quit = False
        end_data_counter = 0

        while not quit:

            key, data = self._recv_data()

            if (data == "END_DATA"):
                end_data_counter += 1
                if (end_data_counter == self.num_workers):
                    quit = True
                    continue

            self._process_data(key, data)

        self._send_data()

        print("Top K reducer finished")

