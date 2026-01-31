import time
from Interfaces import ITask, TaskType
from Models import AAction, Task, EmptyTaskTrigger, CompositeTaskTrigger, ScheduleTaskTrigger

class TurnBackAction(AAction):
    _duration: int

    def __init__(self, signal, firstBuy: bool, duration: bool):
        tasks: list[ITask] = list()
        firstType = TaskType.Buy if firstBuy else TaskType.Sell
        secondType = TaskType.Sell if firstBuy else TaskType.Buy

        tasks.append(Task(firstType, EmptyTaskTrigger()))
        tasks.append(Task(secondType, ScheduleTaskTrigger(time.time() + duration)))

        super().__init__(signal, tasks)

        self._duration = duration

    def getDuration(self) -> int:
        return self._duration
