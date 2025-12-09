from pydantic import BaseModel
from typing import Any, Dict, Optional
import time

from .constants import WorkflowEventType


class WorkflowEvent(BaseModel):
    workflow_name: str
    node_id: str
    event_type: WorkflowEventType
    timestamp: float = time.time()
    attempt: int
    payload: Optional[Dict[str, Any]] = None  # node result or error
