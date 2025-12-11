import json
import uuid
import decimal
import datetime
import pytest
import pydantic as pd

from dag_engine.serializer.json import WorkflowJSONEncoder


class CustomJSON:
    def __json__(self):
        return {"hello": "world"}


class SimpleModel(pd.BaseModel):
    x: int
    y: str


def test_decimal_serialized_to_float():
    obj = {"value": decimal.Decimal("12.34")}
    encoded = json.dumps(obj, cls=WorkflowJSONEncoder)
    assert encoded == '{"value": 12.34}'


def test_uuid_serialized_to_str():
    u = uuid.uuid4()
    obj = {"id": u}
    encoded = json.dumps(obj, cls=WorkflowJSONEncoder)
    assert encoded == f'{{"id": "{str(u)}"}}'


def test_datetime_serialized_iso8601():
    dt = datetime.datetime(2023, 5, 1, 12, 30, 45)
    obj = {"ts": dt}
    encoded = json.dumps(obj, cls=WorkflowJSONEncoder)
    assert encoded == '{"ts": "2023-05-01T12:30:45"}'


def test_date_serialized_iso8601():
    d = datetime.date(2023, 5, 1)
    obj = {"day": d}
    encoded = json.dumps(obj, cls=WorkflowJSONEncoder)
    assert encoded == '{"day": "2023-05-01"}'


def test_pydantic_model_dump_json():
    model = SimpleModel(x=1, y="test")
    obj = {"m": model}
    encoded = json.dumps(obj, cls=WorkflowJSONEncoder)

    # Expected JSON
    assert encoded == '{"m": {"x": 1, "y": "test"}}'


def test_set_serialized_to_string():
    obj = {"s": {1, 2, 3}}
    encoded = json.dumps(obj, cls=WorkflowJSONEncoder)

    # Order is not guaranteed, so parse and validate type
    parsed = json.loads(encoded)
    assert isinstance(parsed["s"], str)  # set converted to string representation


def test_custom_object_json_method():
    obj = {"c": CustomJSON()}
    encoded = json.dumps(obj, cls=WorkflowJSONEncoder)

    assert encoded == '{"c": {"hello": "world"}}'


def test_fallback_to_default():
    """WorkflowJSONEncoder should fallback for unsupported types."""
    class Unserializable:
        pass

    obj = {"u": Unserializable()}

    with pytest.raises(TypeError):
        json.dumps(obj, cls=WorkflowJSONEncoder)
