import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from operations.rows import RowCompareExpander
from operations.counters import LocalTeamCounter
from middleware.connection import WorkerSocket

#
# format(msg) -> home_team home_points away_points away_team
#
class LocalTeamWorker(object):

    def __init__(self, config):
        
        def compare_great_or_equal(v1, v2):
            return v1 >= v2

        self.socket = WorkerSocket(config["worker-local-team"])
        self.row_expander = RowCompareExpander("home_points,away_points",
                                               compare_great_or_equal,
                                               "home_team_won",
                                               "1,0")
        self.counter = LocalTeamCounter()

    def _process_data(self, msg):

        home_team, home_points, away_points, away_team = msg.split(" ")

        row = ["home_points=" + home_points, "away_points=" + away_points]

        row = self.row_expander.expand(row)

        # Keep only the added element
        # 1 or 0 depending on the match result
        row = [row[2]]

        self.counter.count(row)

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


