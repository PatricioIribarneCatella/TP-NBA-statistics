import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from operations.rows import RowReducer
from operations.counters import LocalPointsCounter
from middleware.connection import WorkerSocket

class LocalPointsWorker(object):

    def __init__(self, config):

        self.socket = WorkerSocket(config["worker-local-points"])
        self.row_reducer = RowReducer(["shot_result", "points"])
        self.counter = LocalPointsCounter()

    def _parse_data(self, msg):

        # Split it into the different fields
        msg = msg.split("\n")

        # Pop the last element
        # because the splitter leaves an
        # empty string at the end
        msg.pop()

        return msg

    def _process_data(self, row):

        row = self._parse_data(row)

        row = self.row_reducer.reduce(row)
        self.counter.count(row)

    def run(self):

        print("Local points worker started")

        quit = False
        end_data = False

        while not quit:
            
            socks = self.socket.poll()

            # Message come from the dispatcher
            if self.socket.test(socks, "work"):
                work_msg = self.socket.recv(socks, "work")
                self._process_data(work_msg)
            elif end_data:
                quit = True

            # Message come from dispatcher to end
            if self.socket.test(socks, "control"):
                control_msg = self.socket.recv(socks, "control")
                if control_msg == "0 END_DATA":
                    end_data = True

        # Send result to 'Joiner'
        count = self.counter.get_count()

        self.socket.send("join", "{} {} {} {}".format(
                                count["two_ok"],
                                count["total_two"],
                                count["three_ok"],
                                count["total_three"]))
        
        print("Local points finished")


