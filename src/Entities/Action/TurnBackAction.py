import time
from .Interfaces import ITurnBackAction
from Interfaces import ITask, IAssetPair, ISignal, TaskType
from Entities import AAction, Task
from ValueObjects import EmptyTaskTrigger, ScheduleTaskTrigger

class TurnBackAction(AAction, ITurnBackAction):
    _duration: int

    def __init__(self, signal: ISignal, assetPair: IAssetPair, lotCount: int, firstBuy: bool, duration: int):
        tasks: list[ITask] = list()
        firstType = TaskType.Buy if firstBuy else TaskType.Sell
        secondType = TaskType.Sell if firstBuy else TaskType.Buy

        tasks.append(Task(firstType, assetPair, lotCount, EmptyTaskTrigger()))
        tasks.append(Task(secondType, assetPair, lotCount, ScheduleTaskTrigger(time.time() + duration)))

        super().__init__(signal, tasks)

        self._duration = duration

    def getDuration(self) -> int:
        return self._duration
