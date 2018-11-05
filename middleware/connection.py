import sys
import zmq
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

class ReplicationSocket(object):

    def __init__(self, config):

        # Get the context and create the socket
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)

        # Bind the 'publisher' socket
        net_config = config["bind"]
        self.socket.bind("tcp://{}:{}".format(
                            net_config["ip"],
                            net_config["port"]))

    def send(self, msg):

        self.socket.send_string(msg)

    def close(self):
        self.socket.close()
        self.context.term()

class SuscriberSocket(object):

    def __init__(self, config, topicids):

        # Get the context and create the socket
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        
        # Connect to publisher
        net_config = config["connect"]
        self.socket.connect("tcp://{}:{}".format(
                                net_config["ip"],
                                net_config["port"]))

        # Set the suscriber topics
        for tid in topicids:
            self.socket.setsockopt_string(zmq.SUBSCRIBE, str(tid))

    def recv(self):

        return self.socket.recv_string()
    
    def close(self):
        self.socket.close()
        self.context.term()

class PusherSocket(object):

    def __init__(self):
        
        # Get the context and create the socket
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUSH)

    def send(self, msg):

        self.socket.send_string(msg)

    def close(self):
        self.socket.close()
        self.context.term()

class DispatcherSocket(PusherSocket):

    def __init__(self, config):

        super(DispatcherSocket, self).__init__()

        # Bind the 'dispatcher'/'pusher' socket
        net_config = config["bind"]
        self.socket.bind("tcp://{}:{}".format(
                            net_config["ip"],
                            net_config["port"]))

class ProducerSocket(PusherSocket):

    def __init__(self, config):
        
        super(ProducerSocket, self).__init__()

        # Connect the 'dispatcher'/'pusher' socket
        net_config = config["connect"]
        self.socket.connect("tcp://{}:{}".format(
                                net_config["ip"],
                                net_config["port"]))

class GatherSocket(object):

    def __init__(self, config):

        # Get the context and create the socket
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PULL)
        
        # Bind the 'gather'/'puller' socket
        net_config = config["bind"]
        self.socket.bind("tcp://{}:{}".format(
                            net_config["ip"],
                            net_config["port"]))

    def recv(self):

        return self.socket.recv_string()

    def close(self):
        self.socket.close()
        self.context.term()

class WorkerSocket(object):

    def __init__(self, config):

        # Get the context and create sockets
        self.context = zmq.Context()
        
        net_config = config["nodes"]

        # Channel to receive work
        net_to_filter = net_config["filter-match-summary"]["connect"]
        self.work_socket = self.context.socket(zmq.PULL)
        self.work_socket.connect("tcp://{}:{}".format(
                                    net_to_filter["ip"],
                                    net_to_filter["port"]))

        # Channel to receive stop signal
        self.control_socket = self.context.socket(zmq.SUB)
        self.control_socket.connect("tcp://localhost:7777")
        self.control_socket.setsockopt_string(zmq.SUBSCRIBE, "")

        # Channel to send processed work
        net_to_proxy = net_config["proxy-match-summary"]["connect"]
        self.join_socket = self.context.socket(zmq.PUSH)
        self.join_socket.connect("tcp://{}:{}".format(
                                    net_to_proxy["ip"],
                                    net_to_proxy["port"]))
        
        self.poll_sockets = {
            "work": self.work_socket,
            "control": self.control_socket,
            "join": self.join_socket
        }

        # Poller multiplexer
        self.poller = zmq.Poller()
        self.poller.register(self.work_socket, zmq.POLLIN)
        self.poller.register(self.control_socket, zmq.POLLIN)
        
    # Set the polling with a
    # time-out of 0.5 seconds
    def poll(self):

        return dict(self.poller.poll(500))

    def test(self, sockets, sock_name):

        s = self.poll_sockets[sock_name]

        return sockets.get(s) == zmq.POLLIN

    def recv(self, sockets, sock_name):

        return self.poll_sockets[sock_name].recv_string()

    def send(send, sock_name, data):

        send.poll_sockets[sock_name].send_string(data)

    def close(self):
        self.work_socket.close()
        self.control_socket.close()
        self.join_socket.close()
        self.context.term()


