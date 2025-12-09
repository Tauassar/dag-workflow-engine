
class DagEngineException(Exception):
    ...


class DagValidationError(DagEngineException):
    """Occurs when a DAG Validation fails or DAG construction fails."""
