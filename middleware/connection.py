import sys
import zmq
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import middleware.constants as const

class ReplicationSocket(object):

    def __init__(self, port):

        # Get the context and create the socket
        context = zmq.Context()
        self.socket = context.socket(zmq.PUB)

        # Bind the 'publisher' socket
        self.socket.bind("tcp://0.0.0.0:{}".format(port))

    def send(self, msg):

        self.socket.send_string(msg)


class SuscriberSocket(object):

    def __init__(self, port, topicids):

        # Get the context and create the socket
        context = zmq.Context()
        self.socket = context.socket(zmq.SUB)
        
        # Connect to publisher
        self.socket.connect("tcp://localhost:{}".format(port))

        # Set the suscriber topics
        for tid in topicids:
            self.socket.setsockopt_string(zmq.SUBSCRIBE, str(topicids[tid]))

    def recv(self):

        return self.socket.recv_string()

class DispatcherSocket(object):

    def __init__(self, port):

        # Get the context and create the socket
        context = zmq.Context()
        self.socket = context.socket(zmq.PUSH)

        # Bind the 'dispatcher'/'pusher' socket
        self.socket.bind("tcp://0.0.0.0:{}".format(port))

    def send(self, msg):

        self.socket.send_string(msg)

