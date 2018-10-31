class RowReducer(object):

    def __init__(self, fields):
        self.fields = fields

    # It receives a list of "field=value"
    def reduce(self, row):
        result = []

        for item in row:
            
            field, value = item.split("=")
            
            if field in self.fields:
                result.append(item)

        return result

            


