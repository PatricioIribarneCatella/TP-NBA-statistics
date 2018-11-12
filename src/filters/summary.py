from filters.filter import FilterReplicator

class MatchSummaryFilter(FilterReplicator):

    def __init__(self, input_workers, config):
        super(MatchSummaryFilter, self).__init__(
                            "shot_result=SCORED",
                            "filter-match-summary",
                            input_workers,
                            config
        )

    def run(self):

        super(MatchSummaryFilter, self).run("Match Summary")

