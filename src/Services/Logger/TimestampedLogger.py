from datetime import datetime
import time
from Interfaces import ILogger

class TimestampedLogger(ILogger):
    _logger: ILogger

    def __init__(self, logger: ILogger):
        self._logger = logger

    def init(self):
        self._logger.init()

    def log(self, chunk: str):
        for line in chunk.split('\n'):
            self._logger.log(f"[{datetime.now()}] {line}")
