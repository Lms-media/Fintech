from Interfaces import IPredictor, ILogger, ICandleSeries, IPrediction

class LoggedPredictor(IPredictor):
    _predictor: IPredictor
    _logger: ILogger

    def __init__(self, predictor: IPredictor, logger: ILogger):
        self._predictor = predictor
        self._logger = logger

    def predict(self, input: ICandleSeries) -> IPrediction:
        prediction = self._predictor.predict(input)
        self._logger.log(f"Input: {str(input)}")
        self._logger.log(f"Prediction: {str(prediction)}")

        return prediction
