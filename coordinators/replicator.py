import csv
import glob

#from middleware.connection import ReplicationSocket

class DataReplicator(object):

    def __init__(self, data, pattern, port):
        self.data_path = data
        self.patterns = pattern.strip('[]').replace(' ', '_').split(',')
        #self.socket = ReplicationSocket(port)
        self.NEW_DATA = 1

    def _parse_data(self):

        rows = []

        # Load csv files
        for file_path in glob.glob(self.data_path):
            with open(file_path, newline='') as csvf:
                reader = csv.DictReader(csvf)
                for row in reader:
                    rows.append(row)

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

    def _send_data(self, data):

        for row in data:
            row = self._parse_row(row)
            msg = "{data_id} {data_row}".format(data_id=self.NEW_DATA, data_row=row)
            #self.socket.send(msg)
            print(msg)

    def run(self):

        data = self._parse_data()

        self._send_data(data)

