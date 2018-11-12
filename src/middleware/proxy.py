import zmq

import middleware.constants as const

class Proxy(object):

    def __init__(self, node, config):

        in_config = config[node]["in"]
        out_config = config[node]["out"]
        signal_config = config[node]["signal"]

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

        self.signal_socket = self.context.socket(zmq.PULL)
        self.signal_socket.bind("tcp://{}:{}".format(
                                signal_config["ip"],
                                signal_config["port"]))

        self.poller = zmq.Poller()
        self.poller.register(self.sub_socket, zmq.POLLIN)
        self.poller.register(self.signal_socket, zmq.POLLIN)

    def run(self, node_name):

        print("{} Proxy started".format(node_name))

        quit = False

        while not quit:

            socks = dict(self.poller.poll())

            # Message come from a worker,
            # it has to be dispatch to a reducer
            if socks.get(self.sub_socket) == zmq.POLLIN:
                msg = self.sub_socket.recv_string()
                self.pub_socket.send_string(msg)

            # Message come from a joiner to stop
            if socks.get(self.signal_socket) == zmq.POLLIN:
                end = self.signal_socket.recv_string()
                if end == const.END_DATA:
                    quit = True

        self.close()

        print("{} Proxy finished".format(node_name))

    def close(self):
        self.sub_socket.close()
        self.pub_socket.close()
        self.signal_socket.close()

class MatchSummaryProxy(Proxy):

    def __init__(self, config):
        super(MatchSummaryProxy, self).__init__(
                            "proxy-match-summary",
                            config
        )

    def run(self):

        super(MatchSummaryProxy, self).run("Match Summary")

class TopkProxy(Proxy):

    def __init__(self, config):
        super(TopkProxy, self).__init__(
                        "proxy-topk",
                        config
        )

    def run(self):

        super(TopkProxy, self).run("Top K")

