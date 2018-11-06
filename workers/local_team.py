import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from operations.rows import RowCompareExpander
from operations.counters import LocalTeamCounter
from middleware.connection import WorkerSocket

class LocalTeamWorker(object):

    def __init__(self, config):
        
        def compare_great_or_equal(v1, v2):
            return v1 >= v2

        self.socket = WorkerSocket(config["worker-local-team"])
        self.row_expander = RowCompareExpander("home_team,away_team",
                                               compare_great_or_equal,
                                               "home_team_won",
                                               "1,0")
        self.counter = LocalTeamCounter()

    def run(self):

        print("Local team worker started")
        
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

        self.socket.send("join", "{} {}".format(
                            count["home_count"],
                            count["total_matches"]))

        print("Local team worker finished")


