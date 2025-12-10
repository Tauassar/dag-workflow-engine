
class DagEngineException(Exception):
    ...


class DagValidationError(DagEngineException):
    """Occurs when a DAG Validation fails or DAG construction fails."""


class TemplateResolutionError(DagEngineException):
    """Template is syntactically correct but references a missing key or invalid structure."""
    pass


class MissingDependencyError(DagEngineException):
    """Referenced node has not completed successfully yet."""
    def __init__(self, node_id: str):
        super().__init__(f"Referenced node '{node_id}' is not ready (not SUCCESS)")
        self.node_id = node_id
