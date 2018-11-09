#
# Filter In class
#
class Filter(object):

    def __init__(self, pattern):
        self.field, self.value = pattern.split('=')

    def filter(self, row):
        
        return row[self.field] == self.value
            
