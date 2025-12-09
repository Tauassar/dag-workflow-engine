
class DagEngineException(Exception):
    ...


class DagValidationError(DagEngineException):
    ...


class UndefinedNodeError(DagEngineException):
    ...
