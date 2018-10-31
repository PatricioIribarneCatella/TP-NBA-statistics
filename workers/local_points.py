import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from middleware.connection import SuscriberSocket, DispatcherSocket
import middleware.constants as const

class Dispatcher(object):

    def __init__(self, port, dispatchport):
        
        self.socket = SuscriberSocket(port,
                [const.NEW_DATA, const.END_DATA])
        
        self.dispatchsocket = DispatcherSocket(dispatchport)

    def _recv_data(self):

        msg = self.socket.recv()

        # Split the id from the row
        mid, row = msg.split(" ", 1)

        return mid, row

    def run(self):

        print("Local points dispatcher started")

        mid, row = self._recv_data()

        while (int(mid) == const.NEW_DATA):
            
            self.dispatchsocket.send(row)

            mid, row = self._recv_data()

        print("Local points finished")

