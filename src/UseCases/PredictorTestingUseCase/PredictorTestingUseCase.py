from UseCases import IUseCase
from Entities import INextCandlePrediction
from Interfaces import IDataSource, IPredictor, ILogger
from UseCases.predictionInput import prediction_input
from math import sqrt

class PredictorTestingUseCase(IUseCase):
    _dataSource: IDataSource
    _logger: ILogger
    _predictor: IPredictor[INextCandlePrediction]

    def __init__(self, dataSource: IDataSource, predictor: IPredictor[INextCandlePrediction], logger: ILogger, startIndex=None):
        self._dataSource = dataSource
        self._logger = logger
        self._predictor = predictor
        self._startIndex = startIndex

    def execute(self) -> dict:
        self._dataSource.init()
        candleSeries = self._dataSource.getSeries()
        offset = self._predictor.getCandlesCount()
        start = offset if self._startIndex is None else self._startIndex
        if not offset <= start < candleSeries.getCount():
            raise ValueError("Testing range must contain at least one candle after warmup")
        errors, percentages, baseline_errors, directions = [], [], [], []

        for i in range(start, candleSeries.getCount()):
            trimmedSeries = prediction_input(self._predictor, candleSeries, i)
            predicted = self._predictor.predict(trimmedSeries).getNextCandle()
            actual = candleSeries.getByIndex(i)

            if actual:
                error = predicted.getClosePrice() - actual.getClosePrice()
                reference = (actual.getOpenPrice() if self._predictor.usesCurrentOpen()
                             else trimmedSeries.getByIndex(offset - 1).getClosePrice())
                errors.append(error)
                percentages.append(abs(error) / actual.getClosePrice())
                baseline_errors.append(abs(reference - actual.getClosePrice()))
                sign = lambda x: (x > 0) - (x < 0)
                directions.append(sign(predicted.getClosePrice() - reference) == sign(actual.getClosePrice() - reference))
                self._logger.log(f"timestamp={actual.getOpenTimestamp()} predictedClose={predicted.getClosePrice()} actualClose={actual.getClosePrice()} error={error}")

        count = len(errors)
        self.result = dict(count=count, closeMAE=sum(map(abs, errors)) / count,
                           closeRMSE=sqrt(sum(e * e for e in errors) / count),
                           closeMAPE=100 * sum(percentages) / count,
                           baselineMAE=sum(baseline_errors) / count,
                           directionAccuracy=sum(directions) / count)
        self._logger.log(f"Prediction metrics: {self.result}")
        return self.result
