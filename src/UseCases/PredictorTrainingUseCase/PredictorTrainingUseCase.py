from UseCases import IUseCase
from Interfaces import IDataSource, ILogger
from Entities import TrimmedCandleSeries, INextCandlePrediction
from Contracts import ITrainablePredictor

class PredictorTrainingUseCase(IUseCase):
    _dataSource: IDataSource
    _predictor: ITrainablePredictor[INextCandlePrediction]
    _logger: ILogger

    def __init__(self, dataSource: IDataSource, predictor: ITrainablePredictor[INextCandlePrediction], logger: ILogger, epochs: int = 100):
        self._dataSource = dataSource
        self._predictor = predictor
        self._logger = logger
        self._epochs = epochs

    def execute(self) -> None:
        self._dataSource.init()
        candleSeries = self._dataSource.getSeries()
        offset = self._predictor.getCandlesCount()
        if candleSeries.getCount() <= offset:
            raise ValueError("Training data must contain history and at least one target candle")
        self._predictor.clearDataset()

        for i in range(offset, candleSeries.getCount()):
            trimmed = TrimmedCandleSeries(candleSeries, i - offset, i + 1)
            self._predictor.addDatasetItem(trimmed)

        self._predictor.train(epochs=self._epochs)
        self._logger.log(f"Trained on {candleSeries.getCount() - offset} chronological samples")
