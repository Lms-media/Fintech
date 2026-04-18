from Entities import INextCandlePrediction
from .PercentageDeltaMLPredictorValue import PercentageDeltaMLPredictorValue
from .PercentageDeltaMLPredictorAdapter import PercentageDeltaMLPredictorAdapter
from .PercentageDeltaRNNPredictorAlgo import PercentageDeltaRNNPredictorAlgo
from .PercentageDeltaLSTMPredictorAlgo import PercentageDeltaLSTMPredictorAlgo
from .PercentageDeltaPerceptronPredictorAlgo import PercentageDeltaPerceptronPredictorAlgo
from ..ATrainablePredictor import ATrainablePredictor
from ..Interfaces import ITrainablePredictor
from Interfaces import ICandleSeries

class PercentageDeltaMLPredictor(ATrainablePredictor[PercentageDeltaMLPredictorValue, INextCandlePrediction], ITrainablePredictor):
    _dataset: list[ICandleSeries]

    def __init__(self, candlesCount: int):
        super().__init__(PercentageDeltaLSTMPredictorAlgo(candlesCount), PercentageDeltaMLPredictorAdapter(), candlesCount)
