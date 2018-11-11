import sys
import csv
import glob
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from middleware.connection import DispatcherSocket, GatherSocket, ReplicationSocket
from coordinators.stats import StatsManager

import middleware.constants as const

class DataDispatcher(object):

    def __init__(self, data, stats_path, pattern, num_of_stats, config):
        
        pattern = pattern.strip('[]').replace(' ', '_').split(',')
        self.pattern = dict(list(map(lambda it: (it.split("=")[1], it.split("=")[0]), pattern)))

        self.stats_socket = GatherSocket(config["main"]["stats"])
        self.signal_socket = ReplicationSocket(config["main"]["signal"])
        self.socket = DispatcherSocket(config["main"])
        
        self.data_path = data
        self.num_of_stats = num_of_stats
        self.stats_manager = StatsManager(stats_path)

    def _parse_data(self):

        # Load csv files
        for file_path in glob.glob(self.data_path):
            with open(file_path, newline='') as csvf:
                reader = csv.DictReader(csvf)
                for row in reader:
                    self._send_data(row)

    def _parse_row(self, row):

        new_row = {}

        # Remove all spaces and convert it to '_'
        for key in row.keys():
            new_key = key.replace(' ', '_')
            new_row[new_key] = row[key]

        # Substitute name fields considering
        # the pattern mapping received
        parsed_row = ""

        for item in list(new_row.items()):
            if item[0] in self.pattern:
                field = self.pattern[item[0]]
            else:
                field = item[0]
            parsed_row += field + '=' + item[1] + '\n'

        return parsed_row

    def _send_data(self, row):

        row = self._parse_row(row)
        msg = "{} {}".format(const.NEW_DATA, row)
        self.socket.send(msg)

    def _process_stat(self, msg):

        if msg == "0 END_DATA":
            return

        # Split into the stat id and
        # the data itself
        mid, data = msg.split(" ", 1)

        self.stats_manager.store(mid, data)

    def _receive_statistics(self):

        count = 0

        while count < self.num_of_stats:
            
            msg = self.stats_socket.recv()

            self._process_stat(msg)

            if msg == "0 END_DATA":
                count += 1

    def run(self):

        # Wait for user to start
        input('Enter to start')
        self._parse_data()

        print("\nHolaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n")
        self.signal_socket.send("{} END_DATA".format(const.END_DATA))

        self._receive_statistics()

        # Stats finished
        input('Enter to finish')


