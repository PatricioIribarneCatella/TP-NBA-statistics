import sys
import zmq
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import middleware.constants as const

#
# format(msg) -> two_ok total_two three_ok total_three_points
#
class JoinCounter(object):

    def __init__(self, port):
        context = zmq.Context()
        self.socket = context.socket(zmq.PULL)
        self.socket.bind("tcp://0.0.0.0:{}".format(port))

    def run(self):
        
        print("Join counter started")

        i = 0
        total_two_points = 0
        two_ok = 0
        total_three_points = 0
        three_ok = 0

        while i < 2:
           
            msg = self.socket.recv_string()

            two_scored, total_two, three_scored, total_three = msg.split(" ")

            two_ok += int(two_scored)
            total_two_points += int(total_two)
            three_ok += int(three_scored)
            total_three_points += int(total_three)

            i += 1

        print("2 pts: {}%, 3 pts: {}%".format(two_ok/total_two_points,
                                              three_ok/total_three_points))

        print("Join counter finished")

