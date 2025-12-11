import datetime
import decimal
import json
import uuid
from typing import Any

import pydantic as pd


class WorkflowJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder for workflow node results.

    Converts common non-serializable types:
      - Decimal  -> float
      - UUID     -> str
      - datetime -> ISO8601 string
      - date     -> ISO8601 string
      - BaseModel     -> call obj.model_dump()
      - objects with __json__ -> call obj.__json__()

    Extendable: implement __json__() on custom classes.
    """

    def default(self, obj: Any) -> Any:
        if isinstance(obj, decimal.Decimal):
            return float(obj)

        if isinstance(obj, uuid.UUID):
            return str(obj)

        if isinstance(obj, (datetime.datetime, datetime.date)):
            # ISO 8601 is machine-readable and portable
            return obj.isoformat()

        if isinstance(obj, pd.BaseModel):
            # ISO 8601 is machine-readable and portable
            return obj.model_dump(mode="json")

        # Support custom objects providing __json__()
        if hasattr(obj, "__json__") and callable(obj.__json__):
            return obj.__json__()

        return super().default(obj)
