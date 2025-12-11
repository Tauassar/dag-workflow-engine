import json
import uuid
import decimal
import datetime
import pytest
import pydantic as pd

from dag_engine.serializer.json import JSONResultSerializer


class CustomJSON:
    def __json__(self):
        return {"k": "v"}


class M(pd.BaseModel):
    x: int
    y: str


def test_dumps_returns_bytes():
    s = JSONResultSerializer()
    out = s.dumps({"a": 1})
    assert isinstance(out, bytes)
    assert out == b'{"a": 1}'


def test_dumps_decimal():
    s = JSONResultSerializer()
    data = {"d": decimal.Decimal("10.5")}
    out = s.dumps(data)
    assert out == b'{"d": 10.5}'


def test_dumps_uuid():
    s = JSONResultSerializer()
    u = uuid.uuid4()
    out = s.dumps({"id": u})
    assert out == f'{{"id": "{u}"}}'.encode("utf-8")


def test_dumps_datetime():
    s = JSONResultSerializer()
    dt = datetime.datetime(2024, 1, 1, 10, 30)
    out = s.dumps({"ts": dt})
    assert out == b'{"ts": "2024-01-01T10:30:00"}'


def test_dumps_pydantic_model():
    s = JSONResultSerializer()
    m = M(x=1, y="abc")
    out = s.dumps({"m": m})
    assert out == b'{"m": {"x": 1, "y": "abc"}}'


def test_dumps_custom_json_object():
    s = JSONResultSerializer()
    obj = CustomJSON()
    out = s.dumps({"c": obj})
    assert out == b'{"c": {"k": "v"}}'


def test_loads_returns_python_object():
    s = JSONResultSerializer()
    data = s.loads(b'{"x": 1, "y": "abc"}')
    assert data == {"x": 1, "y": "abc"}


def test_loads_works_after_dumps_roundtrip():
    s = JSONResultSerializer()
    original = {
        "id": str(uuid.uuid4()),
        "nums": [1, 2, 3],
        "nested": {"a": 10},
    }

    dumped = s.dumps(original)
    loaded = s.loads(dumped)

    assert loaded == original


def test_loads_raises_on_invalid_json():
    s = JSONResultSerializer()

    with pytest.raises(json.JSONDecodeError):
        s.loads(b"not valid json")


def test_roundtrip_mixed_complex_types():
    s = JSONResultSerializer()

    u = uuid.uuid4()
    d = decimal.Decimal("3.14")
    dt = datetime.datetime(2025, 5, 17, 11, 22)
    m = M(x=99, y="test")
    c = CustomJSON()

    obj = {
        "uuid": u,
        "decimal": d,
        "datetime": dt,
        "model": m,
        "custom": c,
    }

    dumped = s.dumps(obj)
    loaded = s.loads(dumped)

    # JSONResultSerializer.loads produces JSON-parsed dict (no python objects)
    assert loaded == {
        "uuid": str(u),
        "decimal": float(d),
        "datetime": dt.isoformat(),
        "model": {"x": 99, "y": "test"},
        "custom": {"k": "v"},
    }
