import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from operations.rows import RowMatchExpander
from workers.worker import Worker

class MatchSummaryWorker(Worker):

    def __init__(self, reducers, config):

        super(MatchSummaryWorker, self).__init__(
                        "worker-match-summary", config)

        self.num_reducers = reducers
        self.home_row_expander = RowMatchExpander("home_scored=Yes",
                                             "home_points",
                                             "points,0")

        self.away_row_expander = RowMatchExpander("home_scored=No",
                                             "away_points",
                                             "points,0")

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

    # Encodes data considering that
    # it has to be hashed to send it to
    # the proper reducer
    def _encode_data(self, msg):

        # First hash the key, in this case
        # is (home_team, away_team, date)
        home_team = self._find_item(msg, "home_team")
        away_team = self._find_item(msg, "away_team")
        date = self._find_item(msg, "date")
        home_points = self._find_item(msg, "home_points")
        away_points = self._find_item(msg, "away_points")

        rid = hash("{},{},{}".format(home_team, away_team, date)) % self.num_reducers
        rid += 1

        msg = str(rid) + " "
        msg += "home_team=" + home_team + "\n"
        msg += "away_team=" + away_team + "\n"
        msg += "date=" + date + "\n"
        msg += "home_points=" + home_points + "\n"
        msg += "away_points=" + away_points + "\n"

        return msg

    def _process_data(self, msg):

        msg = self._parse_data(msg)

        msg = self.home_row_expander.expand(msg)
        msg = self.away_row_expander.expand(msg)

        msg = self._encode_data(msg)

        self.socket.send("join", msg)

    def _send_end_signal(self):
        
        # Send 'finish' message to all the reducers
        for r in range(1, self.num_reducers + 1):
            self.socket.send("join", "{rid} END_DATA".format(rid=r))

    def run(self):
       
        super(MatchSummaryWorker, self).run("Match summary")

