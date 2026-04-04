from __future__ import annotations

from typing import Any, Callable, Dict


class Registry:
    def __init__(self) -> None:
        self._items: Dict[str, Callable[..., Any]] = {}

    def register(self, name: str, item: Callable[..., Any]) -> None:
        self._items[name] = item

    def get(self, name: str) -> Callable[..., Any]:
        return self._items[name]

    def items(self) -> Dict[str, Callable[..., Any]]:
        return dict(self._items)

