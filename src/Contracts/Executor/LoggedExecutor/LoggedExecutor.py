from Interfaces import IExecutor, ILogger, IAction

class LoggedExecutor(IExecutor):
    _executor: IExecutor
    _logger: ILogger

    def __init__(self, executor: IExecutor, logger: ILogger):
        self._executor = executor
        self._logger = logger

    def start(self, action: IAction) -> None:
        self._executor.start(action)
        self._logger.log(f"Started Action: {str(action)}")
