import typing as t
import pydantic as pd


class RetryPolicy(pd.BaseModel):
    max_attempts: int = pd.Field(default=3, ge=1)
    initial_backoff_seconds: float = 1.0
    backoff_multiplier: float = 2.0
    max_backoff_seconds: float = 10.0

    def backoff_for_attempt(self, attempt: int) -> float:
        backoff = self.initial_backoff_seconds * (self.backoff_multiplier ** (attempt - 1))
        return min(backoff, self.max_backoff_seconds)


class NodeDefinition(pd.BaseModel):
    id: str
    type: str = pd.Field(alias="handler")
    config: dict[str, t.Any] = pd.Field(default_factory=dict)
    depends_on: list[str] = pd.Field(alias="dependencies", default_factory=list)
    retry_policy: RetryPolicy = RetryPolicy()
    timeout_seconds: float | None = None


class DAGDefinition(pd.BaseModel):
    nodes: list[NodeDefinition]


class WorkflowDefinition(pd.BaseModel):
    name: str
    dag: DAGDefinition
