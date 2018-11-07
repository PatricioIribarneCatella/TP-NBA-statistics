import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from operations.rows import RowReducer
from middleware.connection import WorkerSocket
import middleware.constants as const

class TopkWorker(object):

    def __init__(self, reducers, config):

        self.num_reducers = reducers
        self.socket = WorkerSocket(config["worker-topk"])
        self.row_reducer = RowReducer(["player", "points"])

    def _parse_data(self, msg):

        # Split it into the different fields
        msg = msg.split("\n")

        # Pop the last element
        # because the splitter leaves an
        # empty string at the end
        msg.pop()

        return msg

    def _find_item(self, row, field):

        for item in row:
            f, v = item.split('=')
            if f == field:
                return v

    def _encode_data(self, msg):

        # First hash the key, in this case
        # is (player)
        player = self._find_item(msg, "player")
        points = self._find_item(msg, "points")

        rid = hash("{}".format(player)) % self.num_reducers
        rid += 1

        msg = str(rid) + " "
        msg += "player=" + player + "\n"
        msg += "points=" + points + "\n"

        return msg

    def _process_data(self, msg):

       msg = self._parse_data(msg)

       msg = self.row_reducer.reduce(msg)

       msg = self._encode_data(msg)

       return msg

    def run(self):

        print("Top K worker started")

        quit = False
        end_data = False

        while not quit:

            socks = self.socket.poll()

            # Message come from dispatcher
            if self.socket.test(socks, "work"):
                work_msg = self.socket.recv(socks, "work")
                work_msg = self._process_data(work_msg)
                self.socket.send("join", work_msg)
            elif end_data:
                quit = True

            # Message come from dispatcher to end
            if self.socket.test(socks, "control"):
                control_msg = self.socket.recv(socks, "control")
                if control_msg == "0 END_DATA":
                    end_data = True

        # Send 'finish' message to all the reducers
        for r in range(1, self.num_reducers + 1):
            self.socket.send("join", "{rid} END_DATA".format(rid=r))

        print("Top K worker finished")


