import re
import typing as t
import json

from dag_engine.serializer import WorkflowJSONEncoder

from .exceptions import TemplateResolutionError, MissingDependencyError


class TemplateResolver:
    """
    Resolves {{ node.key.path }} templates in dict/list/string
    by fetching node results from a result_provider callback.

    result_provider(workflow_id, node_id) -> awaitable returning node output

    This class is completely stateless and reusable.
    """

    TEMPLATE_RE = re.compile(r"\{\{\s*([^}]+?)\s*\}\}")

    def __init__(
        self,
        result_provider: t.Callable[[str, str], t.Awaitable[t.Any]],
    ):
        """
        result_provider: async function (workflow_id, node_id) -> result object
        """
        self.result_provider = result_provider

    # ============================================================
    # Public API
    # ============================================================
    async def resolve(self, workflow_id: str, obj: t.Any) -> t.Any:
        """
        Recursively resolves all templates inside `obj`.
        """
        return await self._resolve_in_obj(obj, workflow_id)

    def has_templates(self, obj: t.Any) -> bool:
        """Check if object potentially contains templates."""
        if isinstance(obj, str) and self.TEMPLATE_RE.search(obj):
            return True
        if isinstance(obj, dict):
            return any(self.has_templates(k) or self.has_templates(v) for k, v in obj.items())
        if isinstance(obj, list):
            return any(self.has_templates(i) for i in obj)
        return False

    # ============================================================
    # Core logic
    # ============================================================
    async def _resolve_in_obj(self, obj: t.Any, workflow_id: str) -> t.Any:
        """
        Resolve templates recursively in dict/list/string.
        """
        if isinstance(obj, str):
            return await self._resolve_string(obj, workflow_id)

        elif isinstance(obj, dict):
            result = {}
            for k, v in obj.items():
                new_k = await self._resolve_in_obj(k, workflow_id) if self._has_template(k) else k
                result[new_k] = await self._resolve_in_obj(v, workflow_id)
            return result

        elif isinstance(obj, list):
            return [await self._resolve_in_obj(x, workflow_id) for x in obj]

        return obj  # primitives

    def _has_template(self, obj: t.Any) -> bool:
        return isinstance(obj, str) and self.TEMPLATE_RE.search(obj) is not None

    async def _resolve_string(self, s: str, workflow_id: str) -> t.Any:
        matches = list(self.TEMPLATE_RE.finditer(s))
        if not matches:
            return s

        # entire string is one template => return raw value.
        if len(matches) == 1 and matches[0].span() == (0, len(s)):
            expr = matches[0].group(1).strip()
            return await self._resolve_single_template(workflow_id, expr)

        # else: inline replacements inside a bigger string
        new_s = s
        for m in reversed(matches):  # replace latter matches first
            expr = m.group(1).strip()
            resolved = await self._resolve_single_template(workflow_id, expr)

            if isinstance(resolved, (str, int, float, bool)) or resolved is None:
                token = str(resolved)
            else:
                # Only complex objects become JSON
                token = json.dumps(resolved, cls=WorkflowJSONEncoder)

            start, end = m.span()
            new_s = new_s[:start] + token + new_s[end:]
        return new_s

    # ============================================================
    # Template parsing
    # ============================================================
    async def _resolve_single_template(self, workflow_id: str, expr: str) -> t.Any:
        """
        Resolve a template expression like 'node1.data.value' or 'node1.items.2.x'
        """
        parts = expr.split(".")
        if not parts:
            raise TemplateResolutionError(f"Invalid expression: '{expr}'")

        node_id = parts[0]
        path = parts[1:]

        # Fetch base object (async)
        base = await self.result_provider(workflow_id, node_id)
        if base is None:
            raise MissingDependencyError(node_id)

        return self._resolve_path(base, path)

    def _resolve_path(self, base: t.Any, path_parts: list[str]) -> t.Any:
        cur = base
        for p in path_parts:
            if cur is None:
                raise TemplateResolutionError(f"Cannot resolve '{p}' on None")

            # If list-like, try numeric index
            if isinstance(cur, list):
                try:
                    i = int(p)
                    cur = cur[i]
                    continue
                except Exception:
                    pass  # fallback to dict-like

            # dict lookup
            if isinstance(cur, dict):
                if p not in cur:
                    raise TemplateResolutionError(f"Missing key '{p}' in {cur}")
                cur = cur[p]
                continue

            # Python object attribute?
            if hasattr(cur, p):
                cur = getattr(cur, p)
                continue

            raise TemplateResolutionError(f"Cannot resolve path '{p}' on {cur!r}")

        return cur
