from Interfaces import ILogger

class ParamLogger(ILogger):
    _logger: ILogger
    _value: float
    _step: float

    def __init__(self, logger: ILogger, start: float, step: float):
        self._logger = logger
        self._value = start
        self._step = step

    def init(self):
        self._logger.init()

    def log(self, chunk: str):
        self._logger.log(f"x:{self._value};y:{chunk}")
        self._value += self._step
