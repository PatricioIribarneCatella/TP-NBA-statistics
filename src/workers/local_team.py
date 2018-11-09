import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from src.operations.rows import RowCompareExpander
from src.operations.counters import LocalTeamCounter
from src.workers.worker import Worker

#
# format(msg) -> home_team home_points away_points away_team, date
#
class LocalTeamWorker(Worker):

    def __init__(self, config):
       
        super(LocalTeamWorker, self).__init__(
                        "worker-local-team", config)

        def compare_great_or_equal(v1, v2):
            return v1 >= v2

        self.row_expander = RowCompareExpander("home_points,away_points",
                                               compare_great_or_equal,
                                               "home_team_won",
                                               "1,0")
        self.counter = LocalTeamCounter()


    def _parse_data(self, msg):

        home_team, home_points, away_points, away_team, date = msg.split(" ")

        row = ["home_points=" + home_points, "away_points=" + away_points]

        return row

    def _process_data(self, msg):

        row = self._parse_data(msg)

        row = self.row_expander.expand(row)

        # Keep only the added element
        # 1 or 0 depending on the match result
        row = [row[2]]

        self.counter.count(row)

    def _send_end_signal(self):

        # Send result to 'Joiner'
        count = self.counter.get_count()

        self.socket.send("join", "{} {}".format(
                            count["home_count"],
                            count["total_matches"]))

    def run(self):

        super(LocalTeamWorker, self).run("Local team")

