class BaseException(Exception):
    pass

class APIException(BaseException):
    pass

class ConditionalCheckFailed(APIException):
    root_element = "ConditionalCheckFailed"
    error_msg = "Conditional check failed: %s"
    http_status = '409 Conflict'

class AttributeDoesNotExist(APIException):
    root_element = "AttributeDoesNotExist"
    error_msg = "Attribute (%s) does not exist"
    http_status = '404 Not Found'

    def __init__(self, attribute_name):
        self.msg = self.error_msg % (attribute_name,)
        

class WrongValueFound(ConditionalCheckFailed):
    error_msg = ("Conditional check failed. Attribute (%s) "
                 "value is (%s) but was expected (%s)")

    def __init__(self, attribute_name, expected_value, actual_value):
        self.msg = self.error_msg % (attribute_name,
                                     actual_value,
                                     expected_value)
        
class MultiValuedAttribute(APIException):
    root_element = "MultiValuedAttribute"
    error_msg = ("Attribute (%s) is multi valued. "
                 "Conditional check can only be performed on a "
                 "single-valued attribute")
    http_status = '409 Conflict'

    def __init__(self, attribute_name):
        self.msg = self.error_msg % (attribute_name,)

class FoundUnexpectedAttribute(ConditionalCheckFailed):
    error_msg = "Conditional check failed: Attribute (%s) value exists"

    def __init__(self, attribute_name):
        self.msg = self.error_msg % (attribute_name,)

class InvalidQueryExpression(APIException):
    root_element = "InvalidQueryExpression"
    http_status = '400 Bad Request'

class InvalidSortExpressionException(APIException):
    root_element = "InvalidSortExpression"
    http_status = '400 Bad Request'
