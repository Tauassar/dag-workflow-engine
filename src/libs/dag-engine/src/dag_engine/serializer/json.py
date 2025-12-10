import json
import typing as t

from .encoders import WorkflowJSONEncoder


class JSONResultSerializer:
    """
    Simple JSON serializer using WorkflowJSONEncoder.
    - Primary serializer is JSON
    - Produces UTF-8 bytes
    - Deserialization is strict JSON only
    """

    def __init__(self, encoder: t.Type[json.JSONEncoder] = WorkflowJSONEncoder):
        self.encoder = encoder

    def dumps(self, value: t.Any) -> bytes:
        return json.dumps(value, cls=self.encoder).encode("utf-8")

    def loads(self, data: bytes) -> t.Any:
        return json.loads(data.decode("utf-8"))
