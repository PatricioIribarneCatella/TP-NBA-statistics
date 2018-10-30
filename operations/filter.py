class Filter(object):

    def __init__(self, pattern):
        self.pattern = pattern.split('=')

    def filter(row):
        # [field1=value1, field2=value2, ...]
        items = row.split('\n')
        
        for it in items:
            if it == self.pattern:
                return True

        return False
            
