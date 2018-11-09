import uuid
import src.middleware.constants as const

class StatsManager(object):

    def __init__(self, stats_path):

        self.stats_path = stats_path
        self.stats_id = str(uuid.uuid4())

        self.topk_fname = "topk_"
        self.summary_fname = "mathes_summary_"
        self.local_fname = "local_team_"

        self.handlers = {
            const.TOPK_STAT: self._topk_stat_handler,
            const.MATCH_SUMMARY_STAT: self._match_summary_handler,
            const.LOCAL_POINTS_STAT: self._local_points_handler,
            const.LOCAL_TEAM_STAT: self._local_team_handler
        }

    def _write_stat(self, data, path):

        with open(path, "a+") as f:
            f.write(data + "\n")

    def _topk_stat_handler(self, data):
    
        path = self.stats_path + self.topk_fname + self.stats_id

        self._write_stat(data, path)
        
    def _match_summary_handler(self, data):

        path = self.stats_path + self.summary_fname + self.stats_id

        self._write_stat(data, path)

    def _local_team_handler(self, data):
    
        path = self.stats_path + "local_team_" + self.stats_id

        self._write_stat(data, path)

    def _local_points_handler(self, data):

        self._local_team_handler(data)

    def store(self, stat_id, data):

        handler = self.handlers[stat_id]

        handler(data)

