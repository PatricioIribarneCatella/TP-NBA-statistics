import sys
import csv
import glob
import uuid
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from middleware.connection import ReplicationSocket, GatherSocket
from coordinators.stats import StatsManager
import middleware.constants as const

class DataReplicator(object):

    def __init__(self, data, stats, pattern, config):
        
        self.data_path = data
        self.patterns = pattern.strip('[]').replace(' ', '_').split(',')
        self.stats_socket = GatherSocket(config["main"]["stats"])
        self.socket = ReplicationSocket(config["main"])
        self.MAX_STATS = 4

        self.stats_manager = StatsManager(stats)

    def _parse_data(self):

        rows = []

        # Load csv files
        for file_path in glob.glob(self.data_path):
            with open(file_path, newline='') as csvf:
                reader = csv.DictReader(csvf)
                for row in reader:
                    self._send_data(row)

        return rows

    def _parse_row(self, row):

        new_row = {}

        # Remove all spaces and convert it to '_'
        for key in row.keys():
            new_key = key.replace(' ', '_')
            new_row[new_key] = row[key]

        # Transform row (dictionary) in a string
        # to send, considering the pattern mapping
        parsed_row = ""
        for mapping in self.patterns:
            new, old = mapping.split('=')
            parsed_row += new + '=' + new_row[old] + '\n'

        return parsed_row

    def _send_data(self, row):

        row = self._parse_row(row)
        msg = "{data_id} {data_row}".format(data_id=const.NEW_DATA, data_row=row)
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

        while count < self.MAX_STATS:
            
            msg = self.stats_socket.recv()

            self._process_stat(msg)

            if msg == "0 END_DATA":
                count += 1

    def run(self):

        # Wait for user to start
        input('Enter to start')
        self._parse_data()
        
        self.socket.send("{data_id} END_DATA".format(data_id=const.END_DATA))

        self._receive_statistics()

        # Stats finished
        input('Enter to finish')


