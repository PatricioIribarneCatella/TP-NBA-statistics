import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from middleware.connection import SuscriberSocket, DispatcherSocket
import middleware.constants as const

class MatchSummaryReducer(object):

    def __init__(self, rid, reduce_port, join_port, workers):
        self.reduce_socket = SuscriberSocket(reduce_port, [rid, const.END_DATA])
        self.summary_socket = DispatcherSocket(join_port)
        self.num_workers = workers
        # It stores (key, value) like this:
        #   - key=("home_team"(str), "away_team"(str), "date"(date))
        #   - value=[home_points(int), away_points(int)]
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

        home_points = int(data[3].split("=")[1])
        away_points = int(data[4].split("=")[1])

        return (data[0], data[1], data[2]),[home_points, away_points]

    def _process_data(self, key, data):

        if key in self.data:
            val = self.data[key]
            val[0] += data[0]
            val[1] += data[1]
            self.data[key] = val
        else:
            self.data[key] = data

    def _send_data(self):

        print(self.data)

    def run(self):

        print("Match summary reducer started")

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

        print("Match summary reducer finished")


