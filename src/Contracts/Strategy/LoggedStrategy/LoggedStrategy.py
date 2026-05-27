from Interfaces import IStrategy, ILogger, IPrediction, ISignal

class LoggedStrategy(IStrategy):
    _strategy: IStrategy
    _logger: ILogger

    def __init__(self, strategy: IStrategy, logger: ILogger):
        self._strategy = strategy
        self._logger = logger

    def getSignal(self, input: IPrediction) -> ISignal:
        signal = self._strategy.getSignal(input)
        self._logger.log(f"Input: {str(input)}")
        self._logger.log(f"Signal: {str(signal)}")

        return signal
