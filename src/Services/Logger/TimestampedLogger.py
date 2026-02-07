from datetime import datetime
from Interfaces import ILogger

class TimestampedLogger(ILogger):
    _logger: ILogger

    def __init__(self, logger: ILogger):
        self._logger = logger

    def init(self):
        self._logger.init()

    def log(self, chunk: str):
        self._logger.log(f"[{datetime.now()}] {chunk}")
