class LocalPointsCounter(object):

    def __init__(self):
        
        self.total_two_points = 0
        self.total_three_points = 0
        self.two_points_ok = 0
        self.three_points_ok = 0

    # Receives a list like:
    #
    #  ["shot_result=SCORED", "points=2"]
    #
    def count(self, row):

        f1, v1 = row[0].split("=")
        f2, v2 = row[1].split("=")

        d = {}

        d[f1] = v1
        d[f2] = v2

        if d["points"] == "2":
            self.total_two_points += 1
            if d["shot_result"] == "SCORED":
                self.two_points_ok += 1
        elif d["points"] == "3":
            self.total_three_points += 1
            if d["shot_result"] == "SCORED":
                self.three_points_ok += 1

    def get_count(self):

        return {
            "two_ok": self.two_points_ok,
            "total_two": self.total_two_points,
            "three_ok": self.three_points_ok,
            "total_three": self.total_three_points
        }


