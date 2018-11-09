import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from src.middleware.connection import SuscriberSocket, ProducerSocket

class Reducer(object):

    def __init__(self, rid, workers, node, config):

        reducer_to_proxy = config[node]["nodes"]["proxy"]
        reducer_to_joiner = config[node]["nodes"]["joiner"]

        self.reduce_socket = SuscriberSocket(reducer_to_proxy, [rid])
        self.joiner_socket = ProducerSocket(reducer_to_joiner)

        self.num_workers = workers
        
        self.data = {}

    def _recv_data(self):

        msg = self.reduce_socket.recv()

        msgid, data = msg.split(" ", 1)

        if (data == "END_DATA"):
            return msgid, data

        data = data.split("\n")

        # To take out the last item that it´s
        # an empty string
        data.pop()

        return self._parse_data(data)

    def run(self, node_name):

        print("{} reducer started".format(node_name))

        quit = False
        end_data_counter = 0

        while not quit:

            key, data = self._recv_data()

            if (data == "END_DATA"):
                end_data_counter += 1
                if end_data_counter == self.num_workers:
                    quit = True
                    continue

            self._process_data(key, data)

        self._send_data()
        
        # Send 'end' message
        self.joiner_socket.send("END_DATA")

        print("{} reducer finished".format(node_name))


