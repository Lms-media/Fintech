from typing import Optional
from Interfaces import IContextProvider, ILogger, IExecutionContext, ICandle

class LoggedContextProvider(IContextProvider):
    _contextProvider: IContextProvider
    _logger: ILogger

    def __init__(self, contextProvider: IContextProvider, logger: ILogger):
        self._contextProvider = contextProvider
        self._logger = logger

    def getContext(self, timestamp: int) -> IExecutionContext:
        context = self._contextProvider.getContext(timestamp)
        self._logger.log(f"Got context: {str(context)}")

        return context

    def getNextCandle(self, candle: ICandle) -> Optional[ICandle]:
        return self._contextProvider.getNextCandle(candle)
