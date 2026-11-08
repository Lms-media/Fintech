from Interfaces import TaskType
from ValueObjects import EmptyTaskTrigger
from ..Task import Task
from .AAction import AAction
from .Interfaces import ITurnBackAction


class CandleCorridorAction(AAction, ITurnBackAction):
    """Enter at Open[n], exit at a threshold or Close[n], then remain flat."""
    def __init__(self, signal, assetPair, lotCount, firstBuy, lowLimit, highLimit):
        self._triggerTimestamp = signal.getTimestamp()
        tasks = []
        if lotCount:
            tasks = [
                Task(TaskType.Buy if firstBuy else TaskType.Sell, assetPair, lotCount,
                     EmptyTaskTrigger(), signal.getTimestamp()),
                Task(TaskType.SellCandleCorridor if firstBuy else TaskType.BuyCandleCorridor,
                     assetPair, lotCount, EmptyTaskTrigger(), signal.getTimestamp(), highLimit, lowLimit),
            ]
        super().__init__(signal, tasks)

    def getTriggerTimestamp(self):
        return self._triggerTimestamp
