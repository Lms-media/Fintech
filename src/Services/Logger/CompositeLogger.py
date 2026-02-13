from Interfaces import ILogger

class CompositeLogger(ILogger):
    _loggers: list[ILogger]

    def __init__(self, loggers: list[ILogger]):
        self._loggers = list(loggers)

    def init(self):
        for logger in self._loggers:
            logger.init()

    def log(self, chunk: str):
        for logger in self._loggers:
            logger.log(chunk)
