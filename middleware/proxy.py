import zmq

class WorkerReducerProxy(object):

    def __init__(self, xsub_port, xpub_port):

        # Get the context and create the sockets
        self.context = zmq.Context()
        
        self.sub_socket = self.context.socket(zmq.PULL)
        self.sub_socket.bind("tcp://0.0.0.0:{}".format(xsub_port))

        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.bind("tcp://0.0.0.0:{}".format(xpub_port))

    def run(self):

        while True:

            msg = self.sub_socket.recv_string()
            self.pub_socket.send_string(msg)

    def close(self):
        self.sub_socket.close()
        self.pub_socket.close()
        self.context.term()

