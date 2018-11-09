import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from reducers.reducer import Reducer

class MatchSummaryReducer(Reducer):

    def __init__(self, rid, workers, config):

        super(MatchSummaryReducer, self).__init__(rid,
                                workers, "reducer-match-summary", config)
        
        # It stores (key, value) like this:
            #   - key=("home_team"(str), "away_team"(str), "date"(date))
            #   - value=[home_points(int), away_points(int)]

    def _parse_data(self, data):

        home_points = int(data[3].split("=")[1])
        away_points = int(data[4].split("=")[1])

        return (data[0], data[1], data[2]),[home_points, away_points]

    def _process_data(self, key, data):

        if key in self.data:
            val = self.data[key]
            val[0] += data[0]
            val[1] += data[1]
            self.data[key] = val
        else:
            self.data[key] = data

    def _send_data(self):

        # Send all the reduced data
        for match in self.data.items():
        
            match_info = match[0]
            match_result = match[1]

            home_team = match_info[0].split("=")[1]
            away_team = match_info[1].split("=")[1]
            date = match_info[2].split("=")[1]
            home_points = match_result[0]
            away_points = match_result[1]

            msg = "{} {} {} {}, {}".format(home_team,
                                       home_points,
                                       away_points,
                                       away_team,
                                       date)

            self.joiner_socket.send(msg)

    def run(self):

        super(MatchSummaryReducer, self).run("Match summary")
