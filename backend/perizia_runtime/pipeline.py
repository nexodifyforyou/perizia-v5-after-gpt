from __future__ import annotations

from typing import Callable, List

from perizia_runtime.state import RuntimeState


class PeriziaPipeline:
    def __init__(self, stages: List[Callable[[RuntimeState], None]]) -> None:
        self.stages = stages

    def run(self, state: RuntimeState) -> RuntimeState:
        for stage in self.stages:
            stage(state)
        return state

