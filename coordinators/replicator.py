import glob
import pandas as pd

from middleware.connection import ReplicationSocket

class DataReplicator(object):

    def __init__(self, data, pattern, port):
        self.data_path = data
        self.patterns = pattern.strip('[]').replace(' ', '').split(',')
        self.socket = ReplicationSocket(port)

    def _parse_data(self):

        files = []

        for file_path in glob.glob(self.data_path):
            files.append(pd.read_csv(file_path))

        data = pd.concat(files, ignore_index=True)

        data.columns = data.columns.str.replace(' ', '_')

        for pattern in self.patterns:
            new, old = pattern.split('=')
            data.columns = data.columns.str.replace(old, new)

        return data

    def _send_data(self, data):

        for row in data:
            msg = "{data_id} {data_row}".format(data_id=self.NEW_DATA, data_row=row)
            self.socket.send_string(msg)

    def run(self):

        data = self._parse_data()

        self._send_data(data)

