import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from operations.rows import RowReducer
from middleware.connection import InputWorkerSocket

import middleware.constants as const

class InputDataWorker(object):

    def __init__(self, config):
        
        self.socket = InputWorkerSocket(config["input-worker"])

        self.row_reducer = RowReducer(["home_team",
                                       "away_team",
                                       "home_scored",
                                       "player",
                                       "date",
                                       "points",
                                       "shot_result"])

    def _encode_data(self, data):

        encoded_data = ""

        for item in data:

            encoded_data += item + "\n"

        return encoded_data

    def _process_data(self, msg):
        
        mid, data = msg.split(" ", 1)

        data = data.split("\n")

        # Remove the last item because
        # it´s an empty string
        data.pop()

        data = self.row_reducer.reduce(data)

        return self._encode_data(data)

    def _send_data(self, row):

        msg = "{} {}".format(const.NEW_DATA, row)
        self.socket.send(msg)

    def run(self):

        print("Input worker started")

        quit = False
        end_data = False

        while not quit:

            socks = self.socket.poll()

            # Message come from main dispatcher
            if self.socket.test(socks, "work"):
                work_msg = self.socket.recv(socks, "work")
                work_msg = self._process_data(work_msg)
                self._send_data(work_msg)
            elif end_data:
                quit = True

            # Message come from dispatcher to end
            if self.socket.test(socks, "control"):
                control_msg = self.socket.recv(socks, "control")
                if control_msg == "0 END_DATA":
                    end_data = True
        
        self.socket.send("{} END_DATA".format(const.END_DATA))

        print("Input worker finished")

