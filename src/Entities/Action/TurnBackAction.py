import time
from .Interfaces import ITurnBackAction
from Interfaces import ITask, IAssetPair, ISignal, TaskType
from .AAction import AAction
from ..Task import Task
from ValueObjects import EmptyTaskTrigger, ScheduleTaskTrigger

class TurnBackAction(AAction, ITurnBackAction):
    _triggerTimestamp: int

    def __init__(self, signal: ISignal, assetPair: IAssetPair, lotCount: int, firstBuy: bool, triggerTimestamp: int):
        tasks: list[ITask] = list()
        firstType = TaskType.Buy if firstBuy else TaskType.Sell
        secondType = TaskType.Sell if firstBuy else TaskType.Buy

        tasks.append(Task(firstType, assetPair, lotCount, EmptyTaskTrigger()))
        tasks.append(Task(secondType, assetPair, lotCount, ScheduleTaskTrigger(triggerTimestamp)))

        super().__init__(signal, tasks)

        self._triggerTimestamp = triggerTimestamp

    def getTriggerTimestamp(self) -> int:
        return self._triggerTimestamp
