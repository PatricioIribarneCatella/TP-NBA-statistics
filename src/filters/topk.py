from filters.filter import FilterReplicator

class TopkFilter(FilterReplicator):

    def __init__(self, input_workers, config):
        super(TopkFilter, self).__init__(
                        "shot_result=SCORED",
                        "filter-topk",
                        input_workers,
                        config
        )

    def run(self):

        super(TopkFilter, self).run("Top K")
