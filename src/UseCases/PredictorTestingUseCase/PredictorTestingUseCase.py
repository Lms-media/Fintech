from UseCases import IUseCase
from Entities import INextCandlePrediction, TrimmedCandleSeries
from Interfaces import ICandleSeries, IPredictor, ILogger
from Contracts import MAPredictor

class PredictorTestingUseCase(IUseCase):
    _candleSeries: ICandleSeries
    _logger: ILogger

    def __init__(self, candleSeries: ICandleSeries, logger: ILogger):
        self._candleSeries = candleSeries
        self._logger = logger

    def execute(self) -> None:
        i = 1
        while i <= self._candleSeries.getCount():
            self._singleExecute(i, 0.02)
            i += 1

    def _singleExecute(self, candlesCount: int, sensitivity: float) -> None:
        predictor = MAPredictor(candlesCount, sensitivity)
        toIndex = 2
        totalError = 0
        while toIndex < self._candleSeries.getCount() - 1:
            trimmedSeries = TrimmedCandleSeries(self._candleSeries, 0, toIndex)
            predicted = predictor.predict(trimmedSeries).getNextCandle()
            toIndex += 1
            actual = self._candleSeries.getByIndex(toIndex)

            delta = predicted.getClosePrice() - actual.getClosePrice()
            error = delta * delta
            totalError += error

        self._logger.log(str(totalError))
