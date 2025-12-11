import pytest
import json
import typing as t

from dag_engine.serializer import WorkflowJSONEncoder
from dag_engine.core.templates import TemplateResolver
from dag_engine.core.exceptions import (
    MissingDependencyError,
    TemplateResolutionError
)


async def fake_result_provider(_: str, node_id: str) -> t.Any:
    data = {
        "a": {"value": 123, "nested": {"x": "hello"}},
        "b": ["zero", "one", {"y": 99}],
        "c": {"items": [10, 20, 30]},
        "obj": Dummy(),
    }
    return data.get(node_id)


class Dummy:
    def __init__(self):
        self.attr = "ATTR_VALUE"


@pytest.mark.asyncio
async def test_resolve_entire_string_template():
    r = TemplateResolver(fake_result_provider)
    result = await r.resolve("wf", "{{ a.value }}")
    assert result == 123  # entire string => returns raw type


@pytest.mark.asyncio
async def test_resolve_inline_string_template():
    r = TemplateResolver(fake_result_provider)
    result = await r.resolve("wf", "URL: /api/{{ a.value }}")
    assert result == "URL: /api/123"


@pytest.mark.asyncio
async def test_resolve_multiple_templates():
    r = TemplateResolver(fake_result_provider)
    result = await r.resolve("wf", "{{ a.value }} + {{ a.nested.x }}")
    assert result == "123 + hello"


# ---------------------------------------------------------------------------
# DICT & LIST RECURSION
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_resolve_dict_templates():
    r = TemplateResolver(fake_result_provider)
    obj = {"k1": "{{ a.value }}", "k2": "x{{ a.nested.x }}y"}
    result = await r.resolve("wf", obj)
    assert result == {"k1": 123, "k2": "xhelloy"}


@pytest.mark.asyncio
async def test_resolve_list_templates():
    r = TemplateResolver(fake_result_provider)
    obj = ["{{ a.value }}", "hello", "{{ a.nested.x }}"]
    result = await r.resolve("wf", obj)
    assert result == [123, "hello", "hello"]


@pytest.mark.asyncio
async def test_inline_complex_json_encoded():
    r = TemplateResolver(fake_result_provider)

    obj = "value={{ b.2 }}"
    result = await r.resolve("wf", obj)

    # b[2] = {"y": 99}, so JSON encoded
    assert result == "value=" + json.dumps({"y": 99}, cls=WorkflowJSONEncoder)


@pytest.mark.asyncio
async def test_dict_path_resolution():
    r = TemplateResolver(fake_result_provider)
    result = await r.resolve("wf", "{{ a.nested.x }}")
    assert result == "hello"


@pytest.mark.asyncio
async def test_list_index_path_resolution():
    r = TemplateResolver(fake_result_provider)
    result = await r.resolve("wf", "{{ b.1 }}")
    assert result == "one"


@pytest.mark.asyncio
async def test_list_inside_dict_path_resolution():
    r = TemplateResolver(fake_result_provider)
    result = await r.resolve("wf", "{{ c.items.2 }}")
    assert result == 30


@pytest.mark.asyncio
async def test_object_attribute_resolution():
    r = TemplateResolver(fake_result_provider)
    result = await r.resolve("wf", "{{ obj.attr }}")
    assert result == "ATTR_VALUE"


# ---------------------------------------------------------------------------
# ERROR CASES
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_missing_dependency():
    async def provider(_, node_id):
        return None  # always missing

    r = TemplateResolver(provider)

    with pytest.raises(MissingDependencyError):
        await r.resolve("wf", "{{ missing.value }}")


@pytest.mark.asyncio
async def test_missing_key_in_dict():
    r = TemplateResolver(fake_result_provider)

    with pytest.raises(TemplateResolutionError):
        await r.resolve("wf", "{{ a.not_exist }}")


@pytest.mark.asyncio
async def test_invalid_list_index():
    r = TemplateResolver(fake_result_provider)

    with pytest.raises(TemplateResolutionError):
        await r.resolve("wf", "{{ b.999 }}")


@pytest.mark.asyncio
async def test_invalid_attribute():
    r = TemplateResolver(fake_result_provider)

    with pytest.raises(TemplateResolutionError):
        await r.resolve("wf", "{{ obj.no_such_attribute }}")


@pytest.mark.asyncio
async def test_invalid_expression():
    r = TemplateResolver(fake_result_provider)

    with pytest.raises(MissingDependencyError):
        await r.resolve("wf", "{{ }}")  # empty expression


@pytest.mark.asyncio
async def test_partial_complex_inline_replacement():
    """Ensure complex object is JSON-encoded inside a bigger string."""
    r = TemplateResolver(fake_result_provider)
    result = await r.resolve("wf", "prefix={{ b.2 }}-suffix")

    assert result == "prefix=" + json.dumps({"y": 99}, cls=WorkflowJSONEncoder) + "-suffix"
