from typing import Generic, TypeVar
from Interfaces import IPredictor, ILogger, IReadonlyCandleSeries, IPrediction

P = TypeVar('P', bound=IPredictor)

class LoggedPredictor(IPredictor, Generic[P]):
    _predictor: P
    _logger: ILogger

    def __init__(self, predictor: P, logger: ILogger):
        self._predictor = predictor
        self._logger = logger

    def predict(self, input: IReadonlyCandleSeries) -> IPrediction:
        prediction = self._predictor.predict(input)
        self._logger.log(f"Input: {str(input)}")
        self._logger.log(f"Prediction: {str(prediction)}")

        return prediction

    def getCandlesCount(self):
        return self._predictor.getCandlesCount()
