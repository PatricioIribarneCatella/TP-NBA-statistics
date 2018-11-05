# Contracts a Row in the following way:
#
# - Returns only the items "field=value"
#   in which the 'field' is in 'fields'
#
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

# Expands a Row in the following way:
#
# - Checks if 'check_field' (field=value)
#   is the same and adds a new
#   field 'add_field' with the value
#   contained in the first arg
#   of 'from_field' (vTrue,vFalse)
#
# - If it does not match, adds a new
#   field 'add_field' with the 
#   second arg of 'from_field'
#
class RowExpander(object):

    def __init__(self, check_field, add_field, from_field):
        self.check_field = check_field
        self.add_field = add_field
        self.from_field = from_field

    def _find_item(self, row, field):

        for item in row:
            f, v = item.split('=')
            if f == field:
                return v

    # It receives a list of "field=value"
    def expand(self, row):

        fTrue, vFalse = self.from_field.split(',')

        from_value = self._find_item(row, fTrue)

        if self.check_field in row:
            row.append(self.add_field + '=' + from_value)
        else:
            row.append(self.add_field + '=' + vFalse)

        return row

