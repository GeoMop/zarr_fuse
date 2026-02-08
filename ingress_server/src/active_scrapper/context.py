from dataclasses import dataclass, field
from typing import Any, Dict, Iterable


class ExecutionContextError(RuntimeError):
    """Logical error in ExecutionContext usage."""


@dataclass(frozen=True)
class ExecutionContext:
    values: Dict[str, Any] = field(default_factory=dict)

    def with_value(self, key: str, value: Any) -> "ExecutionContext":
        if key in self.values:
            raise ExecutionContextError(f"Context variable '{key}' already exists")
        return ExecutionContext({**self.values, key: value})

    def with_values(self, mapping: Dict[str, Any]) -> "ExecutionContext":
        overlap = set(mapping) & set(self.values)
        if overlap:
            raise ExecutionContextError(
                f"Context variable(s) already defined: {sorted(overlap)}"
            )
        return ExecutionContext({**self.values, **mapping})

    def branch(self, key: str, values: Iterable[Any]) -> Iterable["ExecutionContext"]:
        if key in self.values:
            raise ExecutionContextError(f"Context variable '{key}' already exists")

        for v in values:
            yield self.with_value(key, v)

    def render(self, template: str) -> str:
        try:
            return template.format(**self.values)
        except KeyError as e:
            raise ExecutionContextError(
                f"Template requires missing context variable: {e.args[0]}"
            ) from e

    def get(self, key: str, default: Any = None) -> Any:
        return self.values.get(key, default)

    def to_dict(self) -> Dict[str, Any]:
        return dict(self.values)

    def require(self, key: str) -> Any:
        if key not in self.values:
            raise ExecutionContextError(f"Missing required context variable: '{key}'")
        return self.values[key]

    def __getitem__(self, key: str) -> Any:
        return self.require(key)

    def __contains__(self, key: str) -> bool:
        return key in self.values

    def __repr__(self) -> str:
        items = ", ".join(f"{k}={v!r}" for k, v in sorted(self.values.items()))
        return f"<ExecutionContext {items}>"
