import time
from .Interfaces import ITurnBackAction
from Interfaces import ITask, IAssetPair, ISignal, IExecutionContext, TaskType
from .AAction import AAction
from ..Task import Task
from ValueObjects import EmptyTaskTrigger, ScheduleTaskTrigger

class TurnBackAction(AAction, ITurnBackAction):
    _triggerTimestamp: int

    def __init__(self, signal: ISignal, assetPair: IAssetPair, lotCount: int, firstBuy: bool, context: IExecutionContext, duration: int, timestamp: int,  interval: int):
        tasks: list[ITask] = list()
        firstType = TaskType.Buy if firstBuy else TaskType.Sell
        secondType = TaskType.Sell if firstBuy else TaskType.Buy

        self._triggerTimestamp = context.getTimestamp() + duration

        tasks.append(Task(firstType, assetPair, lotCount, EmptyTaskTrigger(), timestamp))
        tasks.append(Task(secondType, assetPair, lotCount, ScheduleTaskTrigger(self._triggerTimestamp), timestamp + interval))

        super().__init__(signal, tasks)

    def getTriggerTimestamp(self) -> int:
        return self._triggerTimestamp

    def __str__(self) -> str:
        return super().__str__()
