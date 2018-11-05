import zmq

class WorkerReducerProxy(object):

    def __init__(self, config):

        in_config = config["proxy-match-summary"]["in"]
        out_config = config["proxy-match-summary"]["out"]

        # Get the context and create the sockets
        self.context = zmq.Context()
        
        self.sub_socket = self.context.socket(zmq.PULL)
        self.sub_socket.bind("tcp://{}:{}".format(
                                in_config["ip"],
                                in_config["port"]))

        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.bind("tcp://{}:{}".format(
                                out_config["ip"],
                                out_config["port"]))

    def run(self):

        print("Worker-Reducer Proxy started")

        while True:

            msg = self.sub_socket.recv_string()
            self.pub_socket.send_string(msg)

    def close(self):
        self.sub_socket.close()
        self.pub_socket.close()
        self.context.term()

