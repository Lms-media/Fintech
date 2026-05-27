from Interfaces import IAssessor, ILogger, ISignal, IAction

class LoggedAssessor(IAssessor):
    _assessor: IAssessor
    _logger: ILogger

    def __init__(self, assessor: IAssessor, logger: ILogger):
        self._assessor = assessor
        self._logger = logger

    def getAction(self, input: ISignal) -> IAction:
        action = self._assessor.getAction(input)
        self._logger.log(f"Input: {str(input)}")
        self._logger.log(f"Action: {str(action)}")

        return action
