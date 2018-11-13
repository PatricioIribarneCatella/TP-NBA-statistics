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
        self.socket.setsockopt(zmq.LINGER, -1)

    def send(self, msg):

        self.socket.send_string(msg)

    def close(self):
        self.socket.close()

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
        self.socket.setsockopt(zmq.LINGER, -1)

        # Set the suscriber topics
        for tid in topicids:
            self.socket.setsockopt_string(zmq.SUBSCRIBE, str(tid))

    def recv(self):

        return self.socket.recv_string()
    
    def close(self):
        self.socket.close()

class PusherSocket(object):

    def __init__(self):
        
        # Get the context and create the socket
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUSH)
        self.socket.setsockopt(zmq.LINGER, -1)

    def send(self, msg):

        self.socket.send_string(msg)

    def close(self):
        self.socket.close()

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
        self.socket.setsockopt(zmq.LINGER, -1)

    def recv(self):

        return self.socket.recv_string()

    def close(self):
        self.socket.close()

class InputWorkerSocket(object):

    def __init__(self, config):
        
        # Get the context and create sockets
        self.context = zmq.Context()
        
        net_config = config["nodes"]

        # Channel to receive work
        net_to_dispatcher = net_config["dispatcher"]["connect"]
        self.work_socket = self.context.socket(zmq.PULL)
        self.work_socket.connect("tcp://{}:{}".format(
                                net_to_dispatcher["ip"],
                                net_to_dispatcher["port"]))
        self.work_socket.setsockopt(zmq.LINGER, -1)

        # Channel to receive stop signal
        net_to_signal = net_config["signal"]["connect"]
        self.control_socket = self.context.socket(zmq.SUB)
        self.control_socket.connect("tcp://{}:{}".format(
                                    net_to_signal["ip"],
                                    net_to_signal["port"]))
        self.control_socket.setsockopt_string(zmq.SUBSCRIBE, "")
        self.control_socket.setsockopt(zmq.LINGER, -1)

        # Channels to send processed work
        net_to_filters = net_config["filters"]
        self.sockets = []

        for net in net_to_filters:
            s = self.context.socket(zmq.PUSH)
            s.connect("tcp://{}:{}".format(net["ip"], net["port"]))
            s.setsockopt(zmq.LINGER, -1)
            self.sockets.append(s)

        self.poll_sockets = {
            "work": self.work_socket,
            "control": self.control_socket,
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

    def send(self, data):

        # Send to all waiting filters
        for s in self.sockets:
            s.send_string(data)

    def close(self):
        self.work_socket.close()
        self.control_socket.close()
        for s in self.sockets:
            s.close()

class WorkerSocket(object):

    def __init__(self, config):

        # Get the context and create sockets
        self.context = zmq.Context()
        
        net_config = config["nodes"]

        # Channel to receive work
        net_to_filter = net_config["filter"]["connect"]
        self.work_socket = self.context.socket(zmq.PULL)
        self.work_socket.connect("tcp://{}:{}".format(
                                    net_to_filter["ip"],
                                    net_to_filter["port"]))

        # Channel to receive stop signal
        net_to_signal = net_config["signal"]["connect"]
        self.control_socket = self.context.socket(zmq.SUB)
        self.control_socket.connect("tcp://{}:{}".format(
                                        net_to_signal["ip"],
                                        net_to_signal["port"]))
        self.control_socket.setsockopt_string(zmq.SUBSCRIBE, "")

        # Channel to send processed work
        net_to_proxy = net_config["proxy"]["connect"]
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

    def send(self, sock_name, data):

        self.poll_sockets[sock_name].send_string(data)

    def close(self):
        self.work_socket.close()
        print("holaaaaaaaaaaaaaaaa 11111")
        self.control_socket.close()
        print("holaaaaaaaaaaaaaaaa 22222")
        self.join_socket.close()


