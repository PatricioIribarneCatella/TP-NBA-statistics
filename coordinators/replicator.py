import sys
import csv
import glob
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from middleware.connection import ReplicationSocket
import middleware.constants as const

class DataReplicator(object):

    def __init__(self, data, pattern, port):
        self.data_path = data
        self.patterns = pattern.strip('[]').replace(' ', '_').split(',')
        self.socket = ReplicationSocket(port)

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

    def run(self):

        # Wait for user to start
        input('Enter to start')
        self._parse_data()
        
        self.socket.send("{data_id} END_DATA".format(data_id=const.END_DATA))
        
        # Stats finished
        input('Enter to finish')


