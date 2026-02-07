from Interfaces import IMarket, ILogger, ITask

class LoggedMarket(IMarket):
    _market: IMarket
    _logger: ILogger

    def __init__(self, market: IMarket, logger: ILogger):
        self._market = market
        self._logger = logger

    def execute(self, task: ITask) -> None:
        self._market.execute(task)
        self._logger.log(f"Executed Task: {str(task)}")
