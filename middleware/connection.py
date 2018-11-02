import sys
import zmq
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

class ReplicationSocket(object):

    def __init__(self, port):

        # Get the context and create the socket
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)

        # Bind the 'publisher' socket
        self.socket.bind("tcp://0.0.0.0:{}".format(port))

    def send(self, msg):

        self.socket.send_string(msg)

    def close(self):
        self.socket.close()
        self.context.term()

class SuscriberSocket(object):

    def __init__(self, port, topicids):

        # Get the context and create the socket
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        
        # Connect to publisher
        self.socket.connect("tcp://localhost:{}".format(port))

        # Set the suscriber topics
        for tid in topicids:
            self.socket.setsockopt_string(zmq.SUBSCRIBE, str(tid))

    def recv(self):

        return self.socket.recv_string()
    
    def close(self):
        self.socket.close()
        self.context.term()

class PusherSocket(object):

    def __init__(self, port):
        
        # Get the context and create the socket
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUSH)

    def send(self, msg):

        self.socket.send_string(msg)

    def close(self):
        self.socket.close()
        self.context.term()

class DispatcherSocket(PusherSocket):

    def __init__(self, port):

        super(DispatcherSocket, self).__init__(port)

        # Bind the 'dispatcher'/'pusher' socket
        self.socket.bind("tcp://0.0.0.0:{}".format(port))

class ProducerSocket(PusherSocket):

    def __init__(self, port):
        
        super(ProducerSocket, self).__init__(port)

        # Connect the 'dispatcher'/'pusher' socket
        self.socket.connect("tcp://localhost:{}".format(port))

class GatherSocket(object):

    def __init__(self, port):

        # Get the context and create the socket
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PULL)
        
        # Bind the 'gather'/'puller' socket
        self.socket.bind("tcp://0.0.0.0:{}".format(port))

    def recv(self):

        return self.socket.recv_string()

    def close(self):
        self.socket.close()
        self.context.term()

class WorkerSocket(object):

    def __init__(self, wport, jport):

        # Get the context and create sockets
        self.context = zmq.Context()
        
        # Channel to receive work
        self.work_socket = self.context.socket(zmq.PULL)
        self.work_socket.connect("tcp://localhost:{}".format(wport))

        # Channel to receive stop signal
        self.control_socket = self.context.socket(zmq.SUB)
        self.control_socket.connect("tcp://localhost:7777")
        self.control_socket.setsockopt_string(zmq.SUBSCRIBE, "")

        # Channel to send processed work
        self.join_socket = self.context.socket(zmq.PUSH)
        self.join_socket.connect("tcp://localhost:{}".format(jport))
        
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
