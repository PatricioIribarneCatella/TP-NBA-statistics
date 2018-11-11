from filters.filter import FilterReplicator

class LocalPointsFilter(FilterReplicator):

    def __init__(self, input_workers, config):
        super(LocalPointsFilter, self).__init__(
                            "home_scored=Yes",
                            "filter-local-points",
                            input_workers,
                            config
        )

    def run(self):

        super(LocalPointsFilter, self).run("Local Points")
