import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from src.operations.rows import RowReducer
from src.operations.counters import LocalPointsCounter
from src.workers.worker import Worker

class LocalPointsWorker(Worker):

    def __init__(self, config):

        super(LocalPointsWorker, self).__init__(
                    "worker-local-points", config)

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

    def _send_end_signal(self):

        count = self.counter.get_count()

        # Send result to 'Joiner'
        self.socket.send("join", "{} {} {} {}".format(
                                count["two_ok"],
                                count["total_two"],
                                count["three_ok"],
                                count["total_three"]))

    def run(self):

        super(LocalPointsWorker, self).run("Local points")


