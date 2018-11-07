import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from middleware.connection import SuscriberSocket, ProducerSocket

class MatchSummaryReducer(object):

    def __init__(self, rid, workers, config):

        reducer_to_proxy = config["reducer-match-summary"]["nodes"]["proxy"]
        reducer_to_summary = config["reducer-match-summary"]["nodes"]["joiner"]

        self.reduce_socket = SuscriberSocket(reducer_to_proxy, [rid])
        self.summary_socket = ProducerSocket(reducer_to_summary)
        self.num_workers = workers
        
        # It stores (key, value) like this:
        #   - key=("home_team"(str), "away_team"(str), "date"(date))
        #   - value=[home_points(int), away_points(int)]
        self.data = {}

    def _recv_data(self):

        msg = self.reduce_socket.recv()

        msgid, data = msg.split(" ", 1)

        if (data == "END_DATA"):
            return msgid, data

        data = data.split("\n")

        # To take out the last item that it´s
        # an empty string
        data.pop()

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

            self.summary_socket.send(msg)

        # Send 'end' message
        self.summary_socket.send("END_DATA")

    def run(self):

        print("Match summary reducer started")

        quit = False
        end_data_counter = 0

        while not quit:

            key, data = self._recv_data()

            if (data == "END_DATA"):
                end_data_counter += 1
                if end_data_counter == self.num_workers:
                    quit = True
                    continue

            self._process_data(key, data)

        self._send_data()

        print("Match summary reducer finished")


