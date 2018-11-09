import zmq

class Proxy(object):

    def __init__(self, node, config):

        in_config = config[node]["in"]
        out_config = config[node]["out"]

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

        print("Proxy started")

        while True:

            msg = self.sub_socket.recv_string()
            self.pub_socket.send_string(msg)

    def close(self):
        self.sub_socket.close()
        self.pub_socket.close()
        self.context.term()

class MatchSummaryProxy(Proxy):

    def __init__(self, config):
        super(MatchSummaryProxy, self).__init__(
                            "proxy-match-summary",
                            config
        )

class TopkProxy(Proxy):

    def __init__(self, config):
        super(TopkProxy, self).__init__(
                        "proxy-topk",
                        config
        )

