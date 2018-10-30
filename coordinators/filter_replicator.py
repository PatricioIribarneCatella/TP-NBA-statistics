import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from middleware.connection import SuscriberSocket
import middleware.constants as const

class DataFilterReplicator(object):

    def __init__(self, port):
        self.socket = SuscriberSocket(port, const.NEW_DATA)
        self.filter = Filter("shoot_result=SCORED")

    def _process_data(self, row):
        
        return self.filter.filter(row)

    def _parse_data(self, row):

        row = row.split('\n')

        d = {}

        for item in row:
            k, v = item.split('=')
            d[k] = v

        return d

    def _recv_data(self):

        msg = self.socket.recv()

        return self._parse_data(msg)

    def _send_data(self, row):
        print(row)

    def run(self):
 
        row = self._recv_data()

        while (int(row['id']) == const.NEW_DATA):

            if self._process_data(row)
                self._send_data(row)

            row = self._recv_data()

