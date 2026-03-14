from dataclasses import dataclass, field
from collections.abc import Iterable
from typing import Any, Self


class ExecutionContextError(RuntimeError):
    """Logical error in ExecutionContext usage."""


@dataclass(frozen=True)
class ExecutionContext:
    values: dict[str, Any] = field(default_factory=dict)

    def with_value(self, key: str, value: Any) -> Self:
        if key in self.values:
            raise ExecutionContextError(f"Context variable '{key}' already exists")
        return ExecutionContext({**self.values, key: value})

    def with_values(self, mapping: dict[str, Any]) -> Self:
        overlap = set(mapping) & set(self.values)
        if overlap:
            raise ExecutionContextError(
                f"Context variable(s) already defined: {sorted(overlap)}"
            )
        return ExecutionContext({**self.values, **mapping})

    def branch(self, key: str, values: Iterable[Any]) -> Iterable[Self]:
        if key in self.values:
            raise ExecutionContextError(
                f"Context variable '{key}' already exists"
            )

        for value in values:
            yield self.with_value(key, value)

    def get(self, key: str, default: Any = None) -> Any:
        return self.values.get(key, default)

    def to_dict(self) -> dict[str, Any]:
        return dict(self.values)

    def require(self, key: str) -> Any:
        if key not in self.values:
            raise ExecutionContextError(
                f"Missing required context variable: '{key}'"
            )
        return self.values[key]

    def __getitem__(self, key: str) -> Any:
        return self.require(key)

    def __contains__(self, key: str) -> bool:
        return key in self.values

    def __repr__(self) -> str:
        items = ", ".join(f"{key}={value!r}" for key, value in sorted(self.values.items()))
        return f"<ExecutionContext {items}>"
