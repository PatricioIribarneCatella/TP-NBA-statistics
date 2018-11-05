import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from operations.rows import RowExpander
from operations.counters import LocalTeamCounter
from middleware.connection import WorkerSocket

class LocalTeamWorker(object):

    def __init__(self, config):
        
        self.socket = WorkerSocket(config["worker-local-team"])
        self.row_expander = RowExpander()
        self.counter = LocalTeamCounter()

    def run(self):

