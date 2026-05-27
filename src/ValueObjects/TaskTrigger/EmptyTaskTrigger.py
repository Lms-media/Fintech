from __future__ import annotations
from Interfaces import ITaskTrigger

class EmptyTaskTrigger(ITaskTrigger):

    def isTriggered(self, context) -> bool:
        return True

    def __eq__(self, other) -> bool:
        return isinstance(other, EmptyTaskTrigger)

    def __hash__(self) -> int:
        return hash('EmptyTaskTrigger')

    def __copy__(self) -> ITaskTrigger:
        return EmptyTaskTrigger()

    def __str__(self) -> str:
        return f"🚩 Empty"
