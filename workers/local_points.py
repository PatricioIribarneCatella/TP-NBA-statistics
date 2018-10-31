import sys
import zmq
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from operations.rows import RowReducer
from operations.counters import LocalPointsCounter
#from middleware.connection import WorkerSocket
import middleware.constants as const

class LocalPointsWorker(object):

    def __init__(self, port):

        #self.socket = WorkerSocket(port)
        self.row_reducer = RowReducer(["shot_result", "points"])
        self.counter = LocalPointsCounter()

    def _parse_data(self, msg):

        # Split it into the different fields
        msg = msg.split("\n")

        # Pop the last element
        # because the splitter leaves an
        # empty string at the end
        msg.pop()

        return msg

    def _process_data(self, row):

        row = self._parse_data(row)

        row = self.row_reducer.reduce(row)
        self.counter.count(row)

    def run(self):

        print("Local points worker started")

        context = zmq.Context()

        # Channel to receive work
        work_socket = context.socket(zmq.PULL)
        work_socket.connect("tcp://localhost:6666")

        # Channel to receive 'end' message
        control_socket = context.socket(zmq.SUB)
        control_socket.connect("tcp://localhost:7777")
        control_socket.setsockopt_string(zmq.SUBSCRIBE, "")

        # Channel to send counter result ro 'Joiner'
        join_socket = context.socket(zmq.PUSH)
        join_socket.connect("tcp://localhost:8888")

        # Poller multiplexer
        poller = zmq.Poller()
        poller.register(work_socket, zmq.POLLIN)
        poller.register(control_socket, zmq.POLLIN)

        quit = False
        end_data = False

        while not quit:
            
            # Set the polling with a
            # time-out of 1 second
            socks = dict(poller.poll(1000))

            # Message come from the dispatcher
            if socks.get(work_socket) == zmq.POLLIN:
                work_msg = work_socket.recv_string()
                print(work_msg)
                self._process_data(work_msg)
            elif end_data:
                quit = True

            # Message come from dispatcher to end
            if socks.get(control_socket) == zmq.POLLIN:
                control_msg = control_socket.recv_string()
                if control_msg == "0 END_DATA":
                    end_data = True

        # Send result to 'Joiner'
        count = self.counter.get_count()

        join_socket.send_string("{} {} {} {}".format(
                                count["two_ok"],
                                count["total_two"],
                                count["three_ok"],
                                count["total_three"]))
        
        two_points_stats = count["two_ok"]/count["total_two"]
        three_points_stats = count["three_ok"]/count["total_three"]

        print("2 pts:{}, 3 pts:{}".format(two_points_stats, three_points_stats))
        print("Local points finished")


