import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from middleware.connection import SuscriberSocket, ProducerSocket
from reducers.reducer import Reducer

class TopkReducer(Reducer):

    def __init__(self, rid, workers, k_number, config):

        super(TopkReducer, self).__init__(rid, workers,
                                "reducer-topk", config)

        self.topk_number = k_number
        
        # It stores (key, value) like this:
        # - key="player"(str)
        # - value=points(int)

    def _parse_data(self, data):

        player = data[0].split("=")[1]
        points = int(data[1].split("=")[1])

        return player, points

    def _process_data(self, key, data):

        if key in self.data:
            self.data[key] += data
        else:
            self.data[key] = data

    def _calculate_topk(self):

        items = list(self.data.items())

        s = sorted(items, key=lambda it: it[1], reverse=True)

        return s[:self.topk_number]

    def _send_data(self):
        
        data = self._calculate_topk()

        for info_player in data:
    
            name = info_player[0]
            points = info_player[1]

            msg = "{}\n{}".format(name, points)

            self.joiner_socket.send(msg)

    def run(self):

        super(TopkReducer, self).run("Top K")
