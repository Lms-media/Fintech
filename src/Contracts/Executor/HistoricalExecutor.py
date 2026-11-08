from Interfaces import IExecutor
from Entities.Action.CandleCorridorAction import CandleCorridorAction


class HistoricalExecutor(IExecutor):
    """Sequential bar settlement, without threads or wall-clock delays."""
    def __init__(self, market):
        self._market = market

    def start(self, action):
        if not isinstance(action, CandleCorridorAction):
            raise ValueError("HistoricalExecutor supports CandleCorridorAction only")
        action.start()
        for task in action.getTasks():
            task.unlock()
            self._market.execute(task)
            task.finish()
        action.finish()
