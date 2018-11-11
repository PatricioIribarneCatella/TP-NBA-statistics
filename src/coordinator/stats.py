import uuid
import middleware.constants as const

class StatsManager(object):

    def __init__(self, stats_path):

        self.stats_path = stats_path

        # Generates a unique key to
        # identify this stats
        self.stats_id = str(uuid.uuid4())

        self.topk_fname = "topk_"
        self.summary_fname = "mathes_summary_"
        self.local_fname = "local_team_"
        self.fext = ".stat"

        self.handlers = {
            const.TOPK_STAT: self._topk_stat_handler,
            const.MATCH_SUMMARY_STAT: self._match_summary_handler,
            const.LOCAL_POINTS_STAT: self._local_points_handler,
            const.LOCAL_TEAM_STAT: self._local_team_handler
        }

    def _build_path(self, stat_name):

        return self.stats_path + stat_name + self.stats_id + self.fext

    def _write_stat(self, data, path):

        with open(path, "a+") as f:
            f.write(data + "\n")

    def _topk_stat_handler(self, data):
    
        path = self._build_path(self.topk_fname)

        self._write_stat(data, path)
        
    def _match_summary_handler(self, data):

        path = self._build_path(self.summary_fname)

        self._write_stat(data, path)

    def _local_team_handler(self, data):
    
        path = self._build_path(self.local_fname)

        self._write_stat(data, path)

    def _local_points_handler(self, data):

        self._local_team_handler(data)

    def store(self, stat_id, data):

        handler = self.handlers[stat_id]

        handler(data)

