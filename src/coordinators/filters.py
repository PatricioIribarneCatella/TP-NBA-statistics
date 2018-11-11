import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from middleware.connection import GatherSocket, DispatcherSocket, ReplicationSocket
from operations.filter import Filter

import middleware.constants as const

class FilterReplicator(object):

    def __init__(self, pattern, node, num_input_workers, config):
        
        self.socket = GatherSocket(config[node]["input"])
        
        self.dispatchsocket = DispatcherSocket(
                    config[node])

        # Internal socket to send signal
        # to stop running workers
        self.signalsocket = ReplicationSocket(
                config[node]["signal"])

        self.input_workers = num_input_workers

        self.filter = Filter(pattern)

    def _parse_data(self, msg):

        # Split the id from the row
        mid, row = msg.split(" ", 1)

        if (int(mid) == const.END_DATA):
            return mid, ""

        row = row.split('\n')
        
        # Becuase the last element is an empty
        # string becuase of the splitter 
        row.pop()

        d = {}

        for item in row:
            k, v = item.split('=')
            d[k] = v

        return mid, d

    def _recv_data(self):

        msg = self.socket.recv()

        return self._parse_data(msg)

    def _send_data(self, row):

        msg = ""

        # Convert the row (dictionary) into
        # a string to send
        items = list(row.items())
        
        for it in items:
            msg += it[0] + '=' + it[1] + '\n'
        
        self.dispatchsocket.send(msg)

    def run(self):
 
        print("Filter started")

        quit = False
        count = 0

        while not quit:

            mid, row = self._recv_data()

            if (int(mid) == const.END_DATA):
                count += 1
                if count == self.input_workers:
                    quit = True
                continue

            if self.filter.filter(row):
                self._send_data(row)

        self.signalsocket.send("{} {}".format(const.END_DATA, "END_DATA"))

        print("Filter finished")

class MatchSummaryFilter(FilterReplicator):

    def __init__(self, input_workers, config):
        super(MatchSummaryFilter, self).__init__(
                            "shot_result=SCORED",
                            "filter-match-summary",
                            input_workers,
                            config
        )

class LocalPointsFilter(FilterReplicator):

    def __init__(self, input_workers, config):
        super(LocalPointsFilter, self).__init__(
                            "home_scored=Yes",
                            "filter-local-points",
                            input_workers,
                            config
        )

class TopkFilter(FilterReplicator):

    def __init__(self, input_workers, config):
        super(TopkFilter, self).__init__(
                        "shot_result=SCORED",
                        "filter-topk",
                        input_workers,
                        config
        )

