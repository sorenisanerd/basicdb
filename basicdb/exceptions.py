class BaseException(Exception):
    pass

class APIException(BaseException):
    pass

class ConditionalCheckFailed(APIException):
    root_element = "ConditionalCheckFailed"
    http_status = '409 Conflict'

class InvalidQueryExpression(APIException):
    root_element = "InvalidQueryExpression"
    http_status = '400 Bad Request'
