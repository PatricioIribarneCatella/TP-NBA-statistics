import zmq

import constants as const

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

    def __init__(self, port):

        # Get the context and create the socket
        context = zmq.Context()
        self.socket = context.socket(zmq.SUB)

        # Set the suscriber topic
        self.socket.setsockopt_string(zmq.SUSCRIBE, const.NEW_DATA)
        self.socket.setsockopt_string(zmq.SUSCRIBE, const.END_DATA)

        # Connect to publisher
        self.socket.connect("tcp://localhost:{}".format(port))

    def recv(self):

        return self.socket.recv_string()
