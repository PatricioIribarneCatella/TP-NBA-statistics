import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from middleware.connection import WorkerSocket

import middleware.constants as const

class Worker(object):

    def __init__(self, node, config):

        self.socket = WorkerSocket(config[node])

    def close(self):

        self.socket.close()

    def run(self, node_name):
        
        print("{} worker started".format(node_name))

        quit = False
        end_data = False

        while not quit:

            socks = self.socket.poll()

            # Message come from dispatcher
            if self.socket.test(socks, "work"):
                work_msg = self.socket.recv(socks, "work")
                self._process_data(work_msg)
            elif end_data:
                quit = True

            # Message come from dispatcher to end
            if self.socket.test(socks, "control"):
                control_msg = self.socket.recv(socks, "control")
                if control_msg == "{} {}".format(
                        const.END_DATA_ID, const.END_DATA):
                    end_data = True

        self._send_end_signal()

        self.close()

        print("{} worker finished".format(node_name))


