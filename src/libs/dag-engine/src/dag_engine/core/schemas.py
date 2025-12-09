import typing as t
import pydantic as pd


class RetryPolicy(pd.BaseModel):
    max_attempts: int = pd.Field(1)


class NodeDefinition(pd.BaseModel):
    id: str
    type: str = pd.Field(alias="handler")
    config: dict[str, t.Any] = pd.Field(default_factory=dict)
    depends_on: list[str] = pd.Field(alias="dependencies", default_factory=list)


class DAGDefinition(pd.BaseModel):
    nodes: list[NodeDefinition]


class WorkflowDefinition(pd.BaseModel):
    name: str
    dag: DAGDefinition
