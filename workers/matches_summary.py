import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from operations.rows import RowExpander
from middleware.connection import WorkerSocket
import middleware.constants as const

class MatchSummaryWorker(object):

    def __init__(self, wport, jport, reducers):

        self.num_reducers = reducers
        self.socket = WorkerSocket(wport, jport)
        self.home_row_expander = RowExpander("home_scored=Yes",
                                             "home_points",
                                             "points,0")

        self.away_row_expander = RowExpander("home_scored=No",
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

        return msg

    def run(self):
        
        print("Match summary started")

        quit = False
        end_data = False

        while not quit:

            socks = self.socket.poll()

            # Message come from dispatcher
            if self.socket.test(socks, "work"):
                work_msg = self.socket.recv(socks, "work")
                work_msg = self._process_data(work_msg)
                #self.socket.send("join", work_msg)
                print(work_msg)
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

        print("Match summary worker finished")


