class BaseException(Exception):
    pass

class APIException(BaseException):
    pass

class ConditionalCheckFailed(APIException):
    root_element = "ConditionalCheckFailed"
    http_status = '409 Conflict'
